package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

func main() {
  filePath := "./a.txt"

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		log.Fatal(err)
	}
	fileSize := int(fileInfo.Size())
	fmt.Printf("File Size: %d bytes\n", fileSize)

  processingThreads := 3
  writeChannel := make(chan string, 100)
  var wg sync.WaitGroup 

  for i := 0; i < processingThreads; i++ {
    // fmt.Println("About to launch goroutine ", i)
    offset := fileSize / processingThreads * i
    nextOffset := fileSize / processingThreads * (i + 1)
    fmt.Printf("Byte offset: %d\n", offset)

    go func(threadId int) {
      wg.Add(1)
      f, err := os.Open(filePath)
      defer f.Close()
      defer wg.Done()

      if err != nil {
        log.Fatal(err)
      }
      f.Seek(int64(offset), 0)
      bufReader := bufio.NewReader(f)

      currentPos := offset

      if offset != 0 {
        tmp, err := bufReader.ReadBytes(byte('\n'))
        fmt.Printf("Thread id %d skipped %s\n", threadId, string(tmp))
        currentPos += len(tmp)
        if err == io.EOF {
          fmt.Println("EOF")
          return
        }
        if err != nil {
          log.Fatal(err)
        }
      }

      csvReader := csv.NewReader(bufReader)

      for {
        row, err := csvReader.Read()
        if err != nil {
          log.Fatal(err)
        }
        fmt.Printf("Thread id %d read %v\n", threadId, row)
        if err != nil {
          log.Fatal(err)
        }
        writeChannel <- strconv.Itoa(threadId) + "-" + strings.Join(row, ",") + "\n"
        for _, elem := range row {
          // always, either for comma, or for end of line
          currentPos += len([]byte(elem)) + 1
        }

        if currentPos >= nextOffset {
          return
        }
      }
    }(i)
  }

  go func() {
    wg.Wait()
    close(writeChannel)
  }()

  for result := range writeChannel {
    fmt.Printf("Main thread received %s\n", result)
  }
}
