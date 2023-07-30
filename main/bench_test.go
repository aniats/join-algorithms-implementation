package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func removeFiles(pattern string) {
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println("Removed:", file)
		}
	}
}

func BenchmarkMain(b *testing.B) {
	Settings = settingsType{
		// mode:            SortMergeJoinMode,
		mode:            HashJoinMode,
		condensedPrefix: "condensed_",
		workFolderName: "../preprocessed-small-watdiv/",
		// workFolderName: "../data-for-correctness/",
		// workFolderName: "../preprocessed-large-watdiv",
	}

	Fa = fileAccessor{
		prefix: Settings.workFolderName,
	}


	removeFiles(Settings.workFolderName + "condensed*")
	removeFiles(Settings.workFolderName + "sorted*")
	removeFiles(Settings.workFolderName + "final*")

	solve()
}
