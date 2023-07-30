package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
)

type fileAccessor struct {
	prefix string
}

var Fa = fileAccessor {
  prefix: Settings.workFolderName,
}

func MyOpenFile(filename string) (*os.File, error) {
	return os.OpenFile(
		Fa.makePath(filename),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
}

func (fa fileAccessor) makePath(filename string) string {
	res := path.Join(fa.prefix, fmt.Sprintf("%s.txt", filename)) 
	return res
}

func MakeJoinResultWriter(leftTableFilename, rightTableFilename string) (*bufio.Writer, *os.File, string) {
	resultFilename := fmt.Sprintf("%s__%s", leftTableFilename, rightTableFilename)

	resultFile, err := MyOpenFile(resultFilename)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewWriter(resultFile)

	return r, resultFile, resultFilename
}

func MakeNoBufJoinResultWriter(leftTableFilename, rightTableFilename string) (*os.File, string) {
	resultFilename := fmt.Sprintf("%s__%s", leftTableFilename, rightTableFilename)

	resultFile, err := MyOpenFile(resultFilename)
	if err != nil {
		log.Fatal(err)
	}

	return resultFile, resultFilename
}


func MakeCsvReader(filename string) (*csv.Reader, *os.File) {
  file, err := os.Open(Fa.makePath(filename))
  if err != nil {
    log.Fatal(err)
  }
  r := csv.NewReader(bufio.NewReader(file))

  return r, file
}
