package main

import (
	"bufio"
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

func sortAndDumpChunk(tableFilename string, data [][]string, chunkId, fieldToSortBy int) string {
	compare := func(i, j int) bool {
		return data[i][fieldToSortBy] < data[j][fieldToSortBy]
	}

	sort.Slice(data, compare)

	chunkFilename := fmt.Sprintf("%s_chunk_%v", tableFilename, chunkId)

	chunkFile, err := MyOpenFile(chunkFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkFile.Close()

	w := bufio.NewWriter(chunkFile)
	for _, row := range data {
		_, err := w.WriteString(strings.Join(row, ",") + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}

	w.Flush()

	return chunkFilename
}

func splitTableIntoSortedChunks(tableFilename string, fieldToSortBy int) []string {
	// Off the top of my head, allow to allocate 2GB, why not
	maxChunkSizeInBytes := 2 * 1024 * 1024 * 1024

	r, tableFile := MakeCsvReader(tableFilename)
	defer tableFile.Close()

	chunkNumber := 0
	currentChunkSize := 0
	currentChunk := [][]string{}
	chunkFilenames := []string{}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		// In real world length of the string is not always equal to number of bytes, unicode
		// and stuff. But in our data, it's a good approximation.
		for _, part := range row {
			currentChunkSize += len(part)
		}

		currentChunk = append(currentChunk, row)

		if currentChunkSize > maxChunkSizeInBytes {
			chunkFilenames = append(
				chunkFilenames,
				sortAndDumpChunk(tableFilename, currentChunk, chunkNumber, fieldToSortBy),
			)
			chunkNumber += 1
			currentChunk = [][]string{}
			currentChunkSize = 0
		}
	}

	if currentChunkSize > 0 {
		chunkFilenames = append(
			chunkFilenames,
			sortAndDumpChunk(tableFilename, currentChunk, chunkNumber, fieldToSortBy),
		)
	}

	return chunkFilenames
}

type chunkIndexedRow struct {
	row     []string
	chunkId int
}

type RowHeap struct {
	data          []chunkIndexedRow
	fieldToSortBy int
}

func (h RowHeap) Len() int { return len(h.data) }

func (h RowHeap) Less(i, j int) bool {
	return h.data[i].row[h.fieldToSortBy] < h.data[j].row[h.fieldToSortBy]
}

func (h RowHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *RowHeap) Push(x any) { (*h).data = append((*h).data, x.(chunkIndexedRow)) }

func (h *RowHeap) Pop() any {
	old := *h
	n := len(old.data)
	x := old.data[n-1]
	(*h).data = old.data[:n-1]
	return x
}

func mergeSingleTableChunks(sourceTableName string, tableChunkFilenames []string, fieldToSortBy int) string {
	chunkReaders := []*csv.Reader{}
	chunkFiles := make(map[*csv.Reader]*os.File)

	for _, tableChunkFilename := range tableChunkFilenames {
		curChunkReader, tableChunkFile := MakeCsvReader(tableChunkFilename)

		chunkReaders = append(chunkReaders, curChunkReader)
		chunkFiles[curChunkReader] = tableChunkFile
	}

	h := &RowHeap{
		data:          []chunkIndexedRow{},
		fieldToSortBy: fieldToSortBy,
	}

	for chunkId, chunkReader := range chunkReaders {
		firstChunkRow, err := chunkReader.Read()
		if err != nil {
			log.Fatalf("Failed to read the first line of chunk %s\n", tableChunkFilenames[chunkId])
		}

		heap.Push(h, chunkIndexedRow{
			chunkId: chunkId,
			row:     firstChunkRow,
		})
	}

	resultFilename := fmt.Sprintf("sorted_%s", sourceTableName)
	resultFile, err := MyOpenFile(resultFilename)
	if err != nil {
		log.Fatal(err)
	}
	resultW := bufio.NewWriter(resultFile)

	activeReaders := len(chunkReaders)
	for activeReaders > 0 {
		if h.Len() == 0 {
			log.Fatal("Files remaining, but merge heap empty")
		}

		curSmallestRow := heap.Pop(h).(chunkIndexedRow)
		_, err := resultW.WriteString(strings.Join(curSmallestRow.row, ",") + "\n")
		if err != nil {
			log.Fatal(err)
		}

		newRow, err := chunkReaders[curSmallestRow.chunkId].Read()

		if err != io.EOF {
			heap.Push(h, chunkIndexedRow{
				chunkId: curSmallestRow.chunkId,
				row:     newRow,
			})
		} else {
			chunkFiles[chunkReaders[curSmallestRow.chunkId]].Close()
			activeReaders -= 1
			// TODO delete the file from map, but not really needed
		}
	}

	resultW.Flush()

	if h.Len() > 0 {
		log.Fatal("All files processed, but heap non-empty")
	}

	return resultFilename
}

func advancePast(r *csv.Reader, element string, pos int) ([]string, bool) {
	for {
		row, err := r.Read()
		if err == io.EOF {
			return []string{}, false
		}
		if err != nil {
			log.Fatal(err)
		}

		if row[pos] != element {
			return row, true
		}
	}
}

func mergeTwoTables(leftTable, rightTable string) string {
	lR, lFile := MakeCsvReader(leftTable)
	rR, rFile := MakeCsvReader(rightTable)
	defer lFile.Close()
	defer rFile.Close()

	resultWriter, resultFile, resultFilename := MakeJoinResultWriter(leftTable, rightTable)
	defer resultFile.Close()

	lRow, lErr := lR.Read()
	rRow, rErr := rR.Read()

	if lErr != nil || rErr != nil {
		log.Fatal("Failed to read first lines from two sorted tables")
	}

	var advanced bool
	for {
		for lRow[1] != rRow[0] {
			if lRow[1] < rRow[0] {
				lRow, advanced = advancePast(lR, lRow[1], 1)
				if !advanced {
					break
				}
			} else {
				rRow, advanced = advancePast(rR, rRow[0], 0)
				if !advanced {
					break
				}
			}
		}

		// I have to assume I can fit at least set of leftElems, identical to single element, to memory...
		// Well, I already assume strictly harder thing in hashJoin where I store the entire hashtable, right?
		leftElems := []string{}

		currentMergeElem := lRow[1]

		if lRow[1] != rRow[0] {
			log.Fatal("Two rows not equal after advancement")
		}

		var err error
		for lRow[1] == currentMergeElem {
			leftElems = append(leftElems, lRow[0])
			lRow, err = lR.Read()
			if err == io.EOF {
				break
			}
		}

		for rRow[0] == currentMergeElem {
			for _, leftElem := range leftElems {
				tmp := []string{leftElem}
				tmp = append(tmp, rRow...)
				_, err := resultWriter.WriteString(
					strings.Join(tmp, ",") + "\n")
				if err != nil {
					log.Fatal(err)
				}
			}

			rRow, err = rR.Read()
			if err == io.EOF {
				break
			}
			if rRow[0] != currentMergeElem {
				break
			}
		}

		// There will be no more entries. Rows in one of the table ended, and for more
		// answers we need rows in both tables
		if err == io.EOF {
			break
		}
	}

	resultWriter.Flush()

	return resultFilename
}

func SortMergeJoin(leftTable, rightTable string) string {
	fmt.Printf("Merge-sorting %s and %s...\n", leftTable, rightTable)
	// In L table, we want to sort on the second field [0, 1]
	leftSorted := mergeSingleTableChunks(leftTable, splitTableIntoSortedChunks(leftTable, 1), 1)

	// In R table, we want to sort on the first  field [0, 1]
	rightSorted := mergeSingleTableChunks(rightTable, splitTableIntoSortedChunks(rightTable, 0), 0)

	return mergeTwoTables(leftSorted, rightSorted)
}
