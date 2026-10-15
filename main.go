package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

type FilePart struct {
	start int64
	end   int64
}

const (
	PartName  = "part_"
	PartSufix = ".bin"
)

// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/360/Big_Buck_Bunny_360_10s_1MB.mp4
// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/1080/Big_Buck_Bunny_1080_10s_5MB.mp4
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
		var totaldlownloaded int64
		for n := range progressChan {
			totaldlownloaded += n
			pct := float64(totaldlownloaded) / float64(totalFileSize) * 100

			fmt.Printf("\r\033[KDownload Progress: %.2f%% (%d/%d) bytes", pct, totaldlownloaded, totalFileSize)
		}
		fmt.Println()
		close(progressDoneChan)
	}()

	if !supportRange {
		wg.Add(1)
		fmt.Println("Downloading at once since file doesnt support multiple chunks i.e Range header")
		go download_full(client, url, file, &wg, progressChan)
	} else {
		fmt.Printf("Downloading with %d connections\n", numChunks)
		parts := make(map[string]FilePart)
		chunkSize := totalFileSize / numChunks
		for i := int64(0); i < numChunks; i++ {
			wg.Add(1)
			key := part_name(i)
			starByte := i * chunkSize
			endByte := starByte + chunkSize - 1

			if i == numChunks-1 {
				endByte = totalFileSize - 1
			}

			parts[key] = FilePart{
				start: starByte,
				end:   endByte,
			}

			go download_chunk(client, url, parts[key], file, &wg, progressChan)

		}
	}
	wg.Wait()
	close(progressChan)
	<-progressDoneChan
	file.Close()

	elapsed := time.Since(start)

	fmt.Printf("download took taken %v seconds\n", elapsed.Seconds())
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

func part_name(i any) string {
	return PartName + fmt.Sprintf("%d", i) + PartSufix
}

func download_chunk(c *http.Client, url string, chunk_data FilePart, file *os.File, wg *sync.WaitGroup, progress chan int64) {
	defer wg.Done()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk_data.start, chunk_data.end))
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
	currentWriteOffset := chunk_data.start
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

func download_full(c *http.Client, url string, file *os.File, wg *sync.WaitGroup, progress chan int64) {
	defer wg.Done()

	req, err := http.NewRequest("GET", url, nil)
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
