package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

type FilePart struct {
	start int64
	end   int64
}

var isDebug bool = os.Getenv("DEBUG") == "1"

func debug(str any) {
	if isDebug {
		fmt.Print(str)
	}
}

const (
	PartName  = "part_"
	PartSufix = ".bin"
)

// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/360/Big_Buck_Bunny_360_10s_1MB.mp4
// https://test-videos.co.uk/vids/bigbuckbunny/mp4/av1/360/Big_Buck_Bunny_360_10s_1MB.mp4
func main() {
	start := time.Now()
	argv := os.Args

	if len(argv) < 2 {
		debug("url not provided")
		os.Exit(1)
	}
	url := argv[len(argv)-1]

	client := &http.Client{}

	const minChunkSize = 1 * 1024 * 1024

	numChunks, totaFileSize, _ := determine_chunk(client, url, minChunkSize)
	// fmt.Printf("numChunks %d\n", numChunks)
	parts := make(map[string]FilePart)
	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)

	progressDoneChan := make(chan struct{})
	fmt.Println()
	go func() {
		var totaldlownloaded int64
		for n := range progressChan {
			totaldlownloaded += n
			pct := float64(totaldlownloaded) / float64(totaFileSize) * 100

			fmt.Printf("\r\033[KDownload Progress: %.2f%% (%d/%d) bytes", pct, totaldlownloaded, totaFileSize)
		}
		fmt.Println()
		close(progressDoneChan)
	}()

	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		key := part_name(i)
		starByte := i * minChunkSize
		endByte := starByte + numChunks - 1

		if i == numChunks-1 {
			endByte = totaFileSize - 1
		}

		parts[key] = FilePart{
			start: starByte,
			end:   endByte,
		}

		go download_cunk(client, url, parts[key], key, &wg, progressChan)

	}

	wg.Wait()
	close(progressChan)

	<-progressDoneChan

	merge_parts(getFileName(url), parts)
	clean_up(parts)
	elapsed := time.Since(start)

	fmt.Printf("download took taken %v seconds\n", elapsed.Seconds())
}

func getFileName(url string) string {
	return path.Base(url)
}

func determine_chunk(client *http.Client, url string, minChunkSize int64) (numChunks int64, totalFileSize int64, supportRange bool) {
	res, err := client.Head(url)
	if err != nil {
		return 0, -1, false
	}

	defer res.Body.Close()

	totalFileSize = res.ContentLength

	supportRange = res.Header.Get("Accept-Ranges") == "bytes"

	if !supportRange || totalFileSize <= 0 {
		return 1, totalFileSize, false
	}

	if totalFileSize < minChunkSize {
		return 1, totalFileSize, true
	}

	possibleChunks := totalFileSize / minChunkSize

	maxAllowedChunks := min(int64(runtime.NumCPU()), 8)

	numChunks = min(possibleChunks, maxAllowedChunks)

	numChunks = max(1, numChunks)

	return numChunks, totalFileSize, true
}

func part_name(i any) string {
	return PartName + fmt.Sprintf("%d", i) + PartSufix
}

func clean_up(parts map[string]FilePart) {
	for i := range len(parts) {
		file := part_name(i)
		err := os.Remove(file)
		if err != nil {
			fmt.Printf("Unable to delete : %s", file)
		}
	}
}

func merge_parts(output string, parts map[string]FilePart) {
	out, err := os.Create(output)
	if err != nil {
		panic(err)
	}

	defer out.Close()
	debug("merge files: \n")
	for i := range len(parts) {
		part_name := part_name(i)
		debug(fmt.Sprintf("%s\n", part_name))
		f, err := os.Open(part_name)
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(out, f)
		f.Close()
		if err != nil {
			panic(err)
		}

	}

	debug(fmt.Sprintf("%s created \n", output))
}

func download_cunk(c *http.Client, url string, chunk_data FilePart, part_name string, wg *sync.WaitGroup, progress chan int64) {
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

	file, err := os.Create(part_name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

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
