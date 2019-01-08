package ldio

import (
	"io/ioutil"
	"path/filepath"
)

// FindFiles writes matching files to a given channel
func FindFiles(dir string, recursive bool, shouldProcess func(string) bool, fc chan string) {
	children, err := ioutil.ReadDir(dir)

	if err != nil {
		panic("Could not read dir " + dir)
	}

	for _, child := range children {
		name := filepath.Join(dir, child.Name())
		if child.IsDir() {
			if recursive {
				FindFiles(name, recursive, shouldProcess, fc)
			}
		} else if shouldProcess(name) {
			fc <- name
		}
	}
}
