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

// PrefixWriter makes it easy to prefix lines with a custom prefix. Each time
// it writes a byte after a newline('\n') character it calls PrefixFunc() to get
// the byte slice that should be written. |NeedsPrefix| can be set to true to
// cause a prefix to be written immediately. This is useful for causing a prefix
// to get written on the first line.
type PrefixWriter struct {
	Dest        io.Writer
	PrefixFunc  func(w *PrefixWriter) []byte
	NeedsPrefix bool
	NumLines    uint32
}

// Write() will add a prefix to the beginning of each line. It obtains the
// prefix by call |PrefixFunc(w *PrefixWriter)| before printing out any character
// following a newLine. Callers can force a prefix to be printed out before the
// first character in |data| by setting NeedsPrefix to true. Conversely, callers
// can suppress prefixes from being printed by setting NeedsPrefix to false.

func (w *PrefixWriter) Write(data []byte) (int, error) {
	writtenCnt := 0
	for i, b := range data {
		if w.NeedsPrefix {
			w.NeedsPrefix = false
			d1 := w.PrefixFunc(w)
			cnt, err := w.Dest.Write(d1)
			writtenCnt += cnt
			if err != nil {
				return writtenCnt, err
			}
		}
		if b == byte('\n') {
			w.NumLines++
			w.NeedsPrefix = true
		}
		cnt, err := w.Dest.Write(data[i : i+1])
		writtenCnt += cnt
		if err != nil {
			return writtenCnt, err
		}
	}
	return writtenCnt, nil
}
