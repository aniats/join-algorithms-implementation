package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type JoinMode int

const ( 
	HashJoinMode = iota
	SortMergeJoinMode
)

type settingsType struct {
	mode JoinMode
	condensedPrefix string
	workFolderName string
}

var Settings = settingsType{
	mode: HashJoinMode,
	condensedPrefix: "condensed_",
	// workFolderName: "../data-for-correctness",
	workFolderName: "../preprocessed-small-watdiv",
	// workFolderName: "preprocessed-large-watdiv",
}

type condensationInfo struct {
	objectToInt map[string]int
	intToObject map[int]string
}

func preprocessFiles(filenames []string) condensationInfo {
	condensation := condensationInfo{
		objectToInt: make(map[string]int),
		intToObject: make(map[int]string),
	}

	identifier := 1

	for _, filename := range filenames {
		file, err := os.Open(Fa.makePath(filename))
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			val1, val2 := parse2AttrCsvLine(line)
			tryAddToCondensation(condensation, val1, &identifier)
			tryAddToCondensation(condensation, val2, &identifier)
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
	return condensation
}

func tryAddToCondensation(condensation condensationInfo, val string, identifier *int) {
	if _, ok := condensation.objectToInt[val]; !ok {
		// fmt.Printf("Condensed %s to %v\n", val, *identifier)
		condensation.objectToInt[val] = *identifier
		condensation.intToObject[*identifier] = val
		*identifier++
	}
}

func parse2AttrCsvLine(line string) (string, string) {
	record, err := csv.NewReader(strings.NewReader(line)).Read()
	if err != nil {
		log.Fatal(err)
	}
	if len(record) != 2 {
		log.Fatal(fmt.Errorf("invalid line format: %s", line))
	}
	return record[0], record[1]
}

func condenseFiles(filenames []string, condensation condensationInfo) {
	for _, filename := range filenames {
		file, err := os.Open(Fa.makePath(filename))
		fmt.Println("Condensing ", Fa.makePath(filename))
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		condensedFilename := fmt.Sprintf("%s%s", Settings.condensedPrefix, filename)
		condensedFile, err := os.Create(Fa.makePath(condensedFilename))
		if err != nil {
			log.Fatal(err)
		}
		defer condensedFile.Close()
		writer := csv.NewWriter(condensedFile)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ",")
			condensedParts := make([]string, len(parts))
			for i, part := range parts {
				condensedParts[i] = strconv.Itoa(condensation.objectToInt[part])
			}
			err = writer.Write(condensedParts)
			if err != nil {
				log.Fatal(err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Condensed file created: %s\n", condensedFilename)
	}
}

func decompress(compressedTable, destinationPath string, condensation condensationInfo) {
	compressedTableFile, err := os.Open(Fa.makePath(compressedTable))
	if err != nil {
		log.Fatal(err)
	}
	defer compressedTableFile.Close()

	r := bufio.NewScanner(compressedTableFile)

	destinationTable, err := MyOpenFile(destinationPath)

	if err != nil {
		log.Fatal(err)
	}

	w := bufio.NewWriter(destinationTable)

	defer destinationTable.Close()

	for r.Scan() {
		row := r.Text()
		parts := strings.Split(strings.TrimSuffix(row, "\n"), ",")
		result := make([]string, 0, len(row))
		for _, part := range parts {
			partInt, _ := strconv.Atoi(part)
			decompressed := condensation.intToObject[partInt]
			result = append(result, decompressed)
		}
		line := strings.Join(result, ",") + "\n"
		_, err = w.Write([]byte(line))
		if err != nil {
			log.Fatal(err)
		}
	}

	w.Flush()
}

func solve() {
  log.SetFlags(log.LstdFlags | log.Lshortfile)
	tableNames := []string{
		"hasReview",
		"likes",
		"friendOf",
		"follows",
	}

	condensation := preprocessFiles(tableNames)

	condenseFiles(tableNames, condensation)

	manyAttrTable := fmt.Sprintf("%s%s", Settings.condensedPrefix, tableNames[0])

	for i := 1; i < len(tableNames); i++ {
		twoAttrTable := fmt.Sprintf("%s%s", Settings.condensedPrefix, tableNames[i])

		if Settings.mode == HashJoinMode {
			manyAttrTable = HashJoin(twoAttrTable, manyAttrTable)
		} else if Settings.mode == SortMergeJoinMode {
			manyAttrTable = SortMergeJoin(twoAttrTable, manyAttrTable)
		}
	}

	// decompress(manyAttrTable, "final_result_decompressed", condensation)
}

func main() {
	solve()
}
