package ldio

import "os"

func AssureDirExists(dirPath string) {
	_, err := os.Stat(dirPath)

	if err != nil {
		os.MkdirAll(dirPath, os.ModePerm)
	}
}
