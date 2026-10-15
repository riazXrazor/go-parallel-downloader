# Go Parallel Downloader (`gpd`)

A high-performance, concurrent command-line file downloader written in Go.

This project was built as an exploration into Go's concurrency primitives (`goroutines`, `sync.WaitGroup`, `channels`), POSIX-level concurrent file I/O (`pwrite`/`WriteAt`), and HTTP byte-range specifications (`RFC 7233`).

---

## 🚀 Features

- **Parallel Multi-Chunk Downloads**: Splits large files into distinct byte segments downloaded concurrently using HTTP `Range` headers.
- **Direct Concurrent File Writes**: Pre-allocates the destination file with `file.Truncate` and writes chunks concurrently using thread-safe `file.WriteAt`, completely eliminating temporary chunk files and sequential merge overhead.
- **Adaptive Chunk & Connection Sizing**: Intelligently scales chunk counts (from 1 stream for small files up to a maximum cap of 8 connections) to maximize throughput while avoiding HTTP 429 (Rate Limit) throttling.
- **Decoupled Real-Time Progress Bar**: Uses buffered channels (`chan int64`) to stream download progress updates from concurrent workers to a dedicated rendering goroutine without blocking network I/O.
- **Graceful Fallback**: Automatically checks for `Accept-Ranges: bytes` support via a `HEAD` request and falls back to single-stream download if range requests are unsupported.
- **Integrity Verified**: Includes validation scripts to compare MD5 checksums against sequential single-stream benchmarks.

---

## 🧠 What I Tried & The Evolution of the Tool

Building this tool was an iterative journey that went from a naive, disk-heavy implementation to a clean, zero-merge concurrent architecture.

```
Approach 1 (Naive):
Worker Goroutines ---> [ part_0.bin ] \
Worker Goroutines ---> [ part_1.bin ]  ---> Sequential Merge (io.Copy) ---> Final File ---> Delete Parts
Worker Goroutines ---> [ part_2.bin ] /

Approach 2 (Optimized - Current):
Target File Preallocated (file.Truncate)
Worker Goroutines ===== WriteAt(offset) =====> [ Final File ] (Single Pass, No Temp Files)
```

### 1. Approach 1: Temporary Part Files & Micro-Chunks (`old_main.go`)

In the initial prototype:
- **Micro-Range Loop**: Within each chunk worker, it looped making repeated tiny HTTP GET requests (e.g., 4096 bytes per request).
- **Separate Disk Files**: Each worker wrote its bytes into a temporary part file (`part_0.bin`, `part_1.bin`, etc.).
- **Sequential Stitching**: Once all workers completed, `main` opened each part file sequentially, copied its contents into the target destination file with `io.Copy`, and then deleted the temporary files with `os.Remove`.

#### Why this was problematic:
- **Excessive Network Overhead**: Hundreds of repeated HTTP requests added substantial round-trip latency, TLS negotiation overhead, and header parsing costs.
- **I/O Amplification (Double Disk Writing)**: Every byte was written twice (once to `part_*.bin` and once to the merged file) and read once from disk. This created a major disk I/O bottleneck on large downloads.
- **Cleanup Hazards**: If the program was terminated or crashed halfway, orphaned `part_*.bin` files were left lingering on the filesystem.

---

### 2. Approach 2: Direct Concurrent Writing with `WriteAt` (`main.go`)

To solve the I/O bottleneck, the design was re-architected:
- **Pre-allocation (`file.Truncate`)**: Before launching workers, the target file is created with `os.Create` and immediately expanded to `totalFileSize` with `file.Truncate`.
- **Parallel Random-Access Writing (`file.WriteAt`)**: Go's `(*os.File).WriteAt` utilizes the underlying POSIX `pwrite(2)` system call. Because `WriteAt` operates at a specific offset and does not modify the file's shared read/write seek pointer, **multiple goroutines can safely write to different non-overlapping offsets of the same open file concurrently**.
- **Single HTTP Request Per Chunk**: Each worker sends a single `GET` request with the chunk's entire byte range (`bytes=start-end`) and streams the response body using a reusable 32 KB buffer.
- **`Accept-Encoding: identity`**: Disabled automatic HTTP compression (gzip/deflate) to ensure the server streams exact byte counts corresponding directly to disk offsets.

---

## 💡 What I Learned

### 1. Go Concurrency Primitives
- **`sync.WaitGroup` Coordination**:
  - Initialized with `wg.Add(1)` before spawning goroutines, paired with `defer wg.Done()` inside each worker, and synchronized with `wg.Wait()`.
- **Decoupled Architecture with Channels**:
  - Instead of having workers fight over stdout or mutex locks for UI updates, workers emit byte counts (`progress <- n`) to a buffered channel (`make(chan int64, 100)`).
  - A dedicated background goroutine consumes from `progressChan`, aggregates total bytes, calculates percentage, and renders the progress bar.
- **Clean Goroutine Shutdown**:
  - Closing the buffered channel (`close(progressChan)`) signals the progress worker to flush and terminate, and a synchronization channel (`progressDoneChan`) ensures the final newline is printed before `main` exits.

### 2. HTTP Byte Ranges (`RFC 7233`)
- **Pre-flight Probing**: Sending a `HEAD` request to inspect headers:
  - `Content-Length`: Total size in bytes needed for preallocation.
  - `Accept-Ranges`: Whether the server supports `bytes`.
- **Status Codes**: Successful range requests return `206 Partial Content` rather than `200 OK`.
- **Boundary Precision**: Ensuring the final chunk includes the boundary byte (`endByte = totalFileSize - 1`).

### 3. File System & OS I/O
- Difference between sequential `file.Write()` (which relies on the file descriptor's shared offset pointer and is not concurrency-safe) and `file.WriteAt()` (which uses independent offsets per call and is thread-safe for non-overlapping ranges).
- Avoiding disk fragmentation and intermediate buffer allocation through sparse file truncation.

---

## ⚙️ How It Works

```
                        +----------------------+
                        |      HTTP HEAD       |
                        +----------+-----------+
                                   |
                   +---------------+---------------+
                   | Check Content-Length & Ranges |
                   +---------------+---------------+
                                   |
                    +--------------+--------------+
                    | Sizing & file.Truncate(size)|
                    +--------------+--------------+
                                   |
          +------------------------+------------------------+
          |                        |                        |
+---------v---------+    +---------v---------+    +---------v---------+
| Worker 1 (bytes)  |    | Worker 2 (bytes)  |    | Worker N (bytes)  |
| Range: 0 - 5MB    |    | Range: 5MB - 10MB |    | Range: 10MB - ... |
+---------+---------+    +---------+---------+    +---------+---------+
          |                        |                        |
          | WriteAt(offset)        | WriteAt(offset)        | WriteAt(offset)
          | progress <- n          | progress <- n          | progress <- n
          +------------------------+------------------------+
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
+------------------+                               +------------------+
| Destination File |                               |  progressChan    |
| (Direct Write)   |                               |  (UI Goroutine)  |
+------------------+                               +------------------+
```

---

## 🛠️ Adaptive Chunk Strategy

| Total File Size | Number of Chunks | Chunk Size | Strategy |
| :--- | :--- | :--- | :--- |
| `< 2 MB` | 1 | Full file | Single connection (avoids chunk overhead) |
| `2 MB - 10 MB` | Up to 8 | ~1 MB | Low concurrency |
| `10 MB - 100 MB` | Up to 8 | ~5 MB | Balanced parallel streaming |
| `> 100 MB` | Up to 8 | 10 MB - 20 MB | Maximum throughput |

---

## 💻 Getting Started

### Prerequisites

- [Go](https://go.dev/dl/) 1.22+ installed
- Linux / macOS / Windows (POSIX compliant filesystem recommended)

### Build & Run

Clone the repository and build the binary:

```bash
# Build the binary
go build -o gpd main.go

# Run the downloader
./gpd <FILE_URL>
```

Or run directly with Go:

```bash
go run main.go https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/1080/Big_Buck_Bunny_1080_10s_5MB.mp4
```

### Verification & Testing

Verify that parallel chunk downloads preserve byte integrity:

```bash
# 1. Download sample video
./gpd https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/360/Big_Buck_Bunny_360_10s_1MB.mp4

# 2. Run checksum verification against known good test file
bash ./verify_test_file.sh 1

# Clean up downloaded test files
bash ./clean.sh
```

---

## 📁 Repository Structure

```
parallel-downloader/
├── main.go               # Optimized parallel downloader using WriteAt & preallocation
├── old_main.go           # Initial prototype (micro-chunking & temporary file merging)
├── clean.sh              # Cleanup utility for downloaded media and binaries
├── verify_test_file.sh   # MD5 checksum verification script
├── verify_file/          # Benchmark files for checksum comparison
├── .gitignore            # Git ignore rules for binaries, media, and temp files
├── LICENSE               # MIT License
└── README.md             # Project documentation and learning notes
```

---

## 📄 License

This project is licensed under the [MIT License](file:///home/razor/codeing/parallel-downloader/LICENSE).

