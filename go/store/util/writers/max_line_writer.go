// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package writers

import "io"

var (
	// MaxLinesErr is an instance of MaxLinesError that gets returned by
	// Write() whenever the number of lines written has exceeded the number
	// in |MaxLineWriter.MaxLines|.
	MaxLinesErr = MaxLinesError{"Maximum number of lines written"}
)

// MaxLinesError is the type of error returned by Write() whenever the number
// of lines written has exceeded the number in |MaxLineWriter.MaxLines|.
type MaxLinesError struct {
	msg string
}

func (e MaxLinesError) Error() string { return e.msg }

// MaxLineWriter provides an io.Writer interface that counts the number of lines
// that have been written. It will stop writing and returns an error if the
// number of lines written exceeds the number specified in MaxLineWriter.NumLines.
type MaxLineWriter struct {
	Dest     io.Writer
	MaxLines uint32
	NumLines uint32
}

// Write() stops writing and returns an error if an attempt is made to write
// any byte after |MaxLines| newLines have been written. For example, if MaxLines
// is 1, all bytes will be written up to and including the 1st newline. If there
// are any bytes in |data| after the 1st newline, an error will be returned.
//
// Callers can change the value of |w.MaxLines| before any call to Write().
// Setting MaxLines to 0 will allow any number of newLines.
func (w *MaxLineWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	checkMax := w.MaxLines > 0

	if checkMax && w.NumLines >= w.MaxLines {
		return 0, MaxLinesErr
	}

	var err error
	byteCnt := 0

	for i, b := range data {
		if b == byte('\n') {
			w.NumLines++
			if checkMax && w.NumLines > w.MaxLines {
				err = MaxLinesErr
				break
			}
		} else if checkMax && w.NumLines >= w.MaxLines {
			err = MaxLinesErr
			break
		}
		byteCnt = i
	}

	cnt, err1 := w.Dest.Write(data[:byteCnt+1])
	if err1 != nil {
		return cnt, err1
	}
	return cnt, err
}
