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
	"io"
	"reflect"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/test"
)

func TestErrPreservingReader(t *testing.T) {
	tr := test.NewTestReader(32, 16)
	epr := NewErrPreservingReader(tr)

	read1 := make([]byte, 8)
	_, noErr1 := io.ReadFull(epr, read1)
	read2 := make([]byte, 8)
	_, noErr2 := io.ReadFull(epr, read2)
	read3 := make([]byte, 8)
	_, firstErr := io.ReadFull(epr, read3)
	read4 := make([]byte, 8)
	_, secondErr := io.ReadFull(epr, read4)

	for i := 0; i < 8; i++ {
		if read1[i] != byte(i) || read2[i] != byte(i)+8 {
			t.Error("Unexpected values read.")
		}
	}

	// With io.ReadFull, we expect the buffers to exist but error should be set
	if len(read3) == 0 || len(read4) == 0 {
		t.Error("Expected read buffers to exist.")
	}

	if noErr1 != nil || noErr2 != nil {
		t.Error("Unexpected error.")
	}

	if firstErr == nil || secondErr == nil || epr.Err == nil {
		t.Error("Expected error not received.")
	} else {
		first := firstErr.(*test.TestError).ErrId
		second := secondErr.(*test.TestError).ErrId
		preservedErrID := epr.Err.(*test.TestError).ErrId

		if preservedErrID != first || preservedErrID != second {
			t.Error("Error not preserved properly.")
		}
	}
}

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


