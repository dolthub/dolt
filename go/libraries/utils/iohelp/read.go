// Copyright 2019 Dolthub, Inc.
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
	"bufio"
	"io"
)

// ReadLine will read a line from an unbuffered io.Reader where it considers lines to be separated by newlines (\n).
// The data returned will be a string with \r\n characters removed from the end, a bool which says whether the end of
// the stream has been reached, and any errors that have been encountered (other than eof which is treated as the end of
// the final line)
func ReadLine(br *bufio.Reader) (line string, done bool, err error) {
	line, err = br.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			return "", true, err
		}
	}

	crlfCount := 0
	lineLen := len(line)
	for i := lineLen - 1; i >= 0; i-- {
		ch := line[i]

		if ch == '\r' || ch == '\n' {
			crlfCount++
		} else {
			break
		}
	}

	return line[:lineLen-crlfCount], err != nil, nil
}
