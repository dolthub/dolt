// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package id3

import (
	"io"
	"os"
)

func shiftBytesBack(file *os.File, start, offset int64) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	end := stat.Size()

	wrBuf := make([]byte, offset)
	rdBuf := make([]byte, offset)

	wrOffset := offset
	rdOffset := start

	rn, err := file.ReadAt(wrBuf, rdOffset)
	if err != nil && err != io.EOF {
		panic(err)
	}
	rdOffset += int64(rn)

	for {
		if rdOffset >= end {
			break
		}

		n, err := file.ReadAt(rdBuf, rdOffset)
		if err != nil && err != io.EOF {
			return err
		}

		if rdOffset+int64(n) > end {
			n = int(end - rdOffset)
		}

		if _, err := file.WriteAt(wrBuf[:rn], wrOffset); err != nil {
			return err
		}

		rdOffset += int64(n)
		wrOffset += int64(rn)
		copy(wrBuf, rdBuf)
		rn = n
	}

	if _, err := file.WriteAt(wrBuf[:rn], wrOffset); err != nil {
		return err
	}

	return nil
}
