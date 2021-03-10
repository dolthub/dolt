// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
