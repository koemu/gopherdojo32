package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

const conccurency = 3

// FileInfo is file information
type FileInfo struct {
	URI         string
	Filename    string
	Tempdir     string
	FileSize    int64
	Conccurency int64
}

func main() {
	uri := flag.String("uri", "https://www.example.com/", "URI")
	flag.Parse()

	filename := filepath.Base(*uri)
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		fmt.Println("File exists:" + filename)
		os.Exit(1)
	}

	fileSize, errSize := GetSize(*uri)
	if errSize != nil {
		panic(errSize)
	}

	var fileInfo FileInfo
	fileInfo.URI = *uri
	fileInfo.FileSize = fileSize
	fileInfo.Filename = filename
	fileInfo.Conccurency = conccurency
	errDownload := Download(fileInfo)
	if errDownload != nil {
		panic(errDownload)
	}

	errCombine := CombileFile(fileInfo)
	if errCombine != nil {
		panic(errCombine)
	}
}

// GetSize is get filesize
func GetSize(uri string) (int64, error) {
	headResp, headErr := http.Head(uri)
	if headErr != nil {
		return 0, headErr
	}

	if headResp.Header.Get("Accept-Ranges") != "bytes" || headResp.ContentLength <= 0 {
		return 0, errors.New("並列ダウンロードをサポートしていません")
	}

	return headResp.ContentLength, nil
}

// Download is paralell download file
func Download(fileInfo FileInfo) error {
	procPerSize := fileInfo.FileSize / fileInfo.Conccurency
	errG, ctx := errgroup.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			cancel()
			errG.Go(func() error { return errors.New("処理を中断しました") })
		}
	}()

	for i := int64(0); i < conccurency; i++ {
		index := i // for concurrent request
		errG.Go(func() error {
			req, errReq := http.NewRequest("GET", fileInfo.URI, nil)
			if errReq != nil {
				return errReq
			}

			endSize := (index+1)*procPerSize - 1
			if index+1 == conccurency {
				endSize = fileInfo.FileSize
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", index*procPerSize, endSize))
			req.WithContext(ctx)
			res, errRes := http.DefaultClient.Do(req)
			if errRes != nil {
				fmt.Println("リクエストがキャンセルされました")
				return errRes
			}
			defer res.Body.Close()

			filename := fmt.Sprintf("%s.%d", fileInfo.Filename, index)
			output, errFile := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
			if errFile != nil {
				return errFile
			}
			defer output.Close()

			scanner := bufio.NewScanner(res.Body)
			scanner.Split(bufio.ScanBytes)

			IntterptedScanner := func(ctx context.Context) <-chan int {
				status := make(chan int)
				scan := 0
				if scanner.Scan() {
					scan = 1
				}
				go func() {
					for {
						select {
						case <-ctx.Done():
							status <- -1
						case status <- scan:
						}
					}
				}()
				return status
			}
			for {
				status := <-IntterptedScanner(ctx)
				if status < 0 {
					return errors.New("処理を中断しました")
				}
				if status == 0 {
					break
				}
				_, errOutput := output.Write(scanner.Bytes())
				if errOutput != nil {
					return errOutput
				}
			}

			return nil
		})
	}

	if err := errG.Wait(); err != nil {
		files, _ := filepath.Glob(fmt.Sprintf("%s.*", fileInfo.Filename))
		for _, filename := range files {
			os.Remove(filename)
		}
		return err
	}

	return nil
}

// CombileFile is combine parallel downloaded files
func CombileFile(fileInfo FileInfo) error {
	combinedFilename := fileInfo.Filename
	output, errFile := os.OpenFile(combinedFilename, os.O_CREATE|os.O_WRONLY, 0644)
	if errFile != nil {
		return errFile
	}
	defer output.Close()

	for i := int64(0); i < conccurency; i++ {
		filename := fmt.Sprintf("%s.%d", fileInfo.Filename, i)
		input, errInput := os.Open(filename)
		if errInput != nil {
			return errInput
		}
		defer input.Close()
		reader := bufio.NewReader(input)

		_, errCopy := io.Copy(output, reader)
		if errCopy != nil {
			return errCopy
		}
		input.Close()

		os.Remove(filename)
	}

	return nil
}
