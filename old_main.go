//go:build ignore

/**
* this is unoptimized initial try go wrong many things
**/
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
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

func main() {
	start := time.Now()
	argv := os.Args

	if len(argv) < 2 {
		debug("url not provided")
		os.Exit(1)
	}
	url := argv[len(argv)-1]

	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		debug("ueq not valid")
	}

	client := &http.Client{}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	filesize := res.Header["Content-Length"][0]
	range_support := res.Header["Accept-Ranges"][0]
	if range_support != "bytes" {
		debug("does not supports range")
		os.Exit(1)
	}

	filesize_int, _ := strconv.ParseInt(filesize, 10, 64)

	num_of_chunks := int64(runtime.NumCPU())
	chunk_size := (filesize_int / num_of_chunks)
	fmt.Printf("num_of_chunks %d chunk_size %d", num_of_chunks, chunk_size)
	debug(fmt.Sprintf("total bytes = %s\n", filesize))
	debug(fmt.Sprintf("total = %d\n", filesize_int))
	parts := make(map[string]FilePart)
	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)

	progressDoneChan := make(chan struct{})
	fmt.Println()
	go func() {
		var totaldlownloaded int64
		for n := range progressChan {
			totaldlownloaded += n
			pct := float64(totaldlownloaded) / float64(filesize_int) * 100

			fmt.Printf("\r\033[KDownload Progress: %.2f%% (%d/%d) bytes", pct, totaldlownloaded, filesize_int)
		}
		fmt.Println()
		close(progressDoneChan)
	}()

	for i := int64(0); i < num_of_chunks; i++ {
		wg.Add(1)
		key := part_name(i)

		end := i*chunk_size + chunk_size - 1

		if i == num_of_chunks-1 {
			end = end + (filesize_int - end) + 1
		}

		parts[key] = FilePart{
			start: i * chunk_size,
			end:   end,
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
		defer f.Close()
		if err != nil {
			panic(err)
		}

	}

	debug(fmt.Sprintf("%s created \n", output))
}

func download_cunk(c *http.Client, url string, chunk_data FilePart, part string, wg *sync.WaitGroup, progress chan int64) {
	defer wg.Done()
	file_chunk_start := chunk_data.start
	file_chunk_end := chunk_data.end

	chunk_range := int64(4096)

	chunk_size := file_chunk_end - file_chunk_start

	downloaded := int64(0)
	start_range := file_chunk_start
	end_range := int64(0)

	file, err := os.OpenFile(part, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for (file_chunk_end - end_range) > 0 {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			debug("req not valid")
		}

		end_range = start_range + chunk_range - 1

		if end_range > file_chunk_end {
			end_range = start_range + (file_chunk_end - start_range)
			debug(end_range)
			debug("-->last chunk")
		}

		if end_range < 0 {
			debug(fmt.Sprintf("%v\n", chunk_data))
			debug(end_range)
			panic("end range wrong")
		}

		if start_range > end_range {
			debug(fmt.Sprintf("start is more the end %v \n", chunk_size))
			debug(start_range)
			debug(end_range)
			break
		}

		range_bytes := fmt.Sprintf("bytes=%d-%d", start_range, end_range)
		debug(range_bytes)
		req.Header.Add("Range", range_bytes)

		res, err := c.Do(req)
		if err != nil {
			panic("chunk download failed:" + err.Error())
		}

		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			panic("body read" + err.Error())
		}

		start_range = end_range + 1

		_, err = file.Write(body)
		if err != nil {
			panic(part + " write error :" + err.Error())
		}

		progress <- int64(len(body))

		if isDebug {
			downloaded += int64(len(body))
			debug(fmt.Sprintf("Body:%v\n", len(body)))
			progress := float64(downloaded) / float64(chunk_size)
			debug(fmt.Sprintf("Progress : %v \n", progress*100))
		}
	}
}
