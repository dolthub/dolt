package iohelp

import (
	"fmt"
	"io"
	"os"
)

func CopyFile(src, dest string) (size int64, err error) {
	stat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !stat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	srcf, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer func() {
		closeErr := srcf.Close()

		if err == nil {
			err = closeErr
		}
	}()

	destf, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer func() {
		closeErr := destf.Close()

		if err == nil {
			err = closeErr
		}
	}()

	return io.Copy(destf, srcf)
}
