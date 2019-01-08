package ldio

import "os"

func Exists(path string) (bool, bool) {
	stat, err := os.Stat(path)

	if err != nil {
		return false, false
	}

	return true, stat.IsDir()
}
