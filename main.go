package main

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

type ChunkSlice struct {
	start int64
	end   int64
}

// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/360/Big_Buck_Bunny_360_10s_1MB.mp4
// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/1080/Big_Buck_Bunny_1080_10s_5MB.mp4
// https://testfileorg.netwet.net/500MB-CZIPtestfile.org.zip
// https://pub-821312cfd07a4061bf7b99c1f23ed29b.r2.dev/3dicons-png-dynamic-1.0.0.zip 1.26 GB
func main() {
	start := time.Now()
	argv := os.Args

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Failed:", r)
		}
	}()

	if len(argv) < 2 {
		panic("url not provided !!")
	}
	url := argv[len(argv)-1]

	client := &http.Client{}

	res, err := client.Head(url)
	if err != nil {
		panic("invalid url or unable to download !!")
	}

	defer res.Body.Close()

	totalFileSize := res.ContentLength

	supportRange := res.Header.Get("Accept-Ranges") == "bytes"

	numChunks := calculateChunks(totalFileSize)

	filename := getFileName(url)

	file, err := os.Create(filename)
	if err != nil {
		panic("failed to create file: " + err.Error())
	}

	if err := file.Truncate(totalFileSize); err != nil {
		panic("failed to truncate file: " + err.Error())
	}

	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)

	progressDoneChan := make(chan struct{})
	go func() {
		var totalDownloaded int64
		for n := range progressChan {
			totalDownloaded += n
			progressPercent := float64(totalDownloaded) / float64(totalFileSize) * 100
			progressBar := ""
			progressBarMaxWidth := 50
			progressBarLen := progressPercent * float64(progressBarMaxWidth) / 100
			for i := 1; i <= progressBarMaxWidth; i++ {
				if i <= int(progressBarLen) {
					progressBar += "█"
				} else {
					progressBar += "▒"
				}
			}

			fmt.Fprintf(os.Stdout, "\033[KProgress: %s\033[1B\r", progressBar)
			fmt.Fprintf(os.Stdout, "\033[KDownload Progress: %.2f%% (%s/%s)\033[1A\r", progressPercent, HumanReadableBytes(totalDownloaded), HumanReadableBytes(totalFileSize))
		}
		fmt.Println()
		fmt.Println()
		close(progressDoneChan)
	}()

	if !supportRange {
		wg.Add(1)
		fmt.Println("Downloading at once since file does not support multiple chunks i.e Range header")
		go downloadFull(client, url, file, &wg, progressChan)
	} else {
		fmt.Printf("Downloading with %d connections\n", numChunks)
		chunkSize := totalFileSize / numChunks
		for i := int64(0); i < numChunks; i++ {
			wg.Add(1)
			starByte := i * chunkSize
			endByte := starByte + chunkSize - 1

			if i == numChunks-1 {
				endByte = totalFileSize - 1
			}

			part := ChunkSlice{
				start: starByte,
				end:   endByte,
			}

			go downloadChunk(client, url, part, file, &wg, progressChan)

		}
	}
	wg.Wait()
	close(progressChan)
	<-progressDoneChan
	file.Close()

	elapsed := time.Since(start)

	fmt.Printf("Download took %s \n", elapsed.Round(time.Millisecond).String())
}

func HumanReadableBytes(bytes int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	if bytes == 0 {
		return "0 B"
	}
	exp := int(math.Log(float64(bytes)) / math.Log(1024))
	if exp >= len(units) {
		exp = len(units) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(bytes)/math.Pow(1024, float64(exp)), units[exp])
}

func getFileName(url string) string {
	return path.Base(url)
}

func calculateChunks(totalFileSize int64) int64 {
	maxConnections := int64(8) // Sane upper cap to avoid 429s

	switch {
	case totalFileSize < 2*1024*1024: // < 2 MB: Single stream is fastest
		return 1

	case totalFileSize < 10*1024*1024: // 2 MB - 10 MB: 1 MB chunks
		chunks := totalFileSize / (1 * 1024 * 1024)
		return min(chunks, maxConnections)

	case totalFileSize < 100*1024*1024: // 10 MB - 100 MB: 5 MB chunks
		chunks := totalFileSize / (5 * 1024 * 1024)
		return min(chunks, maxConnections)

	default: // > 100 MB: 10 MB - 20 MB chunks
		chunks := totalFileSize / (10 * 1024 * 1024)
		return min(chunks, maxConnections)
	}
}

func downloadChunk(c *http.Client, url string, chunkData ChunkSlice, file *os.File, wg *sync.WaitGroup, progress chan int64) {
	defer wg.Done()

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunkData.start, chunkData.end))
	req.Header.Set("Accept-Encoding", "identity")

	res, err := c.Do(req)
	if err != nil {
		panic("chunk download failed:" + err.Error())
	}

	if res.StatusCode != http.StatusPartialContent && res.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("server returned %s", res.Status))
	}

	defer res.Body.Close()

	buf := make([]byte, 32*1024)
	currentWriteOffset := chunkData.start
	for {
		n, err := res.Body.Read(buf)

		if n > 0 {
			if _, wErr := file.WriteAt(buf[:n], currentWriteOffset); wErr != nil {
				panic(wErr)
			}
			currentWriteOffset += int64(n)
			progress <- int64(n)
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
}

func downloadFull(c *http.Client, url string, file *os.File, wg *sync.WaitGroup, progress chan<- int64) {
	defer wg.Done()

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Accept-Encoding", "identity")

	res, err := c.Do(req)
	if err != nil {
		panic("full download failed:" + err.Error())
	}

	if res.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("server returned %s", res.Status))
	}

	defer res.Body.Close()

	buf := make([]byte, 32*1024)

	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			if _, wErr := file.Write(buf[:n]); wErr != nil {
				panic(wErr)
			}
			progress <- int64(n)
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
}
