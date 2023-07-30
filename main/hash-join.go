package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

func HashJoin(leftTable, rightTable string) string {
	fmt.Printf("Trying to join %s with %s...\n", leftTable, rightTable)
	tLeftRowHash := make(map[int][]int, 50000)

	leftReader, leftTableFile := MakeCsvReader(leftTable)
	defer leftTableFile.Close()

	for {
		leftRow, err := leftReader.Read()
		if err == io.EOF {
			break
		}
		fst, _ := strconv.Atoi(leftRow[0])
		snd, _ := strconv.Atoi(leftRow[1])
		_, exists := tLeftRowHash[snd]
		if !exists {
			tLeftRowHash[snd] = make([]int, 0, 5)
		}

		tLeftRowHash[snd] = append(tLeftRowHash[snd], fst)
	}

	fileInfo, err := os.Stat(Fa.makePath(rightTable))
	if err != nil {
		log.Fatal(err)
	}
	fileSize := int(fileInfo.Size())

	processingThreads := 4
	var wg sync.WaitGroup
	var lock = &sync.Mutex{}

	for i := 0; i < processingThreads; i++ {
		offset := fileSize / processingThreads * i
		nextOffset := fileSize / processingThreads * (i + 1)

		wg.Add(1)
		go func(threadId int) {
			resultFile, _ := MakeNoBufJoinResultWriter(leftTable, rightTable)
			defer resultFile.Close()

			f, err := os.Open(Fa.makePath(rightTable))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			defer wg.Done()
			if err != nil {
				log.Fatal(err)
			}
			var bufReader *bufio.Reader

			shouldSkip := true
			if offset == 0 {
				shouldSkip = false
				_, err = f.Seek(0, 0)
				if err != nil {
					log.Fatal(err)
				}
				bufReader = bufio.NewReader(f)
			} else {
				_, err = f.Seek(int64(offset-1), 0)
				if err != nil {
					log.Fatal(err)
				}
				bufReader = bufio.NewReader(f)
				b, err := bufReader.ReadByte()
				if err != nil {
					log.Fatal(err)
				}
				if b == byte('\n') {
					shouldSkip = false
				}
			}

			currentPos := offset
			if shouldSkip {
				tmp, err := bufReader.ReadBytes(byte('\n'))
				currentPos += len(tmp)
				if err == io.EOF {
					return
				}
				if err != nil {
					log.Fatal(err)
				}
			}

			csvReader := csv.NewReader(bufReader)

			var b bytes.Buffer
			b.Grow(10000)

			for {
				rightRow, err := csvReader.Read()

				if err == io.EOF {
					writeWithLock(lock, &b, resultFile)
					return
				}

				if err != nil {
					log.Fatal(err)
				}

				rightRowFirstAttr, _ := strconv.Atoi(rightRow[0])
				if leftFirstColList, exists := tLeftRowHash[rightRowFirstAttr]; exists {
					for _, possibleLeftFirstCol := range leftFirstColList {

						b.WriteString(strconv.Itoa(possibleLeftFirstCol))
						for _, row := range rightRow {
							b.WriteString(",")
							b.WriteString(row)
						}
						b.WriteString("\n")

						if b.Len() > 9000 {
							writeWithLock(lock, &b, resultFile)
						}
					}
				}

				for _, elem := range rightRow {
					// always, either for comma, or for end of line
					currentPos += len([]byte(elem)) + 1
				}

				if currentPos >= nextOffset {
					writeWithLock(lock, &b, resultFile)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	return fmt.Sprintf("%s__%s", leftTable, rightTable)
}

func writeWithLock(lock *sync.Mutex, b *bytes.Buffer, f *os.File) {
	if b.Len() > 0 {
		lock.Lock()
		b.WriteTo(f)
		lock.Unlock()
	}
	b.Reset()
}
