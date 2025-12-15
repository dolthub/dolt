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
	"bytes"
	"reflect"
	"testing"
)

var rlTests = []struct {
	inputStr      string
	expectedLines []string
}{
	{"line 1\nline 2\r\nline 3\n", []string{"line 1", "line 2", "line 3", ""}},
	{"line 1\nline 2\r\nline 3", []string{"line 1", "line 2", "line 3"}},
	{"\r\nline 1\nline 2\r\nline 3\r\r\r\n\n", []string{"", "line 1", "line 2", "line 3", "", ""}},
}

func TestReadReadLineFunctions(t *testing.T) {
	for _, test := range rlTests {
		bufferedTest := getTestReadLineClosure(test.inputStr)

		testReadLineFunctions(t, "buffered", test.expectedLines, bufferedTest)
	}
}

func getTestReadLineClosure(inputStr string) func() (string, bool, error) {
	r := bytes.NewReader([]byte(inputStr))
	br := bufio.NewReader(r)

	return func() (string, bool, error) {
		return ReadLine(br)
	}
}

func testReadLineFunctions(t *testing.T, testType string, expected []string, rlFunc func() (string, bool, error)) {
	var isDone bool
	var line string
	var err error

	lines := make([]string, 0, len(expected))
	for !isDone {
		line, isDone, err = rlFunc()

		if err == nil {
			lines = append(lines, line)
		}
	}

	if !reflect.DeepEqual(lines, expected) {
		t.Error("Received unexpected results.")
	}
}
