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

package test

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// test your tests so you can test while you test

func TestLDTestUtils(t *testing.T) {
	dir, err := ChangeToTestDir(t.TempDir(), "TestLDTestUtils")

	if err != nil {
		t.Fatal("Couldn't change to test dir")
	}

	dataSize := 32
	data := RandomData(dataSize)

	if len(data) != dataSize {
		t.Error("Wrong amount of data")
	}

	fName := "test.data"
	err = os.WriteFile(fName, data, os.ModePerm)

	if err != nil {
		t.Fatal("Couldn't write to current directory")
	}

	absPath := filepath.Join(dir, fName)

	fInfo, err := os.Stat(absPath)

	if err != nil {
		t.Error("File not where expected")
	} else if fInfo.Size() != int64(dataSize) {
		t.Error("File not of expected size")
	}
}

func TestTestReader(t *testing.T) {
	{
		dest := make([]byte, 32)
		tr := NewTestReader(32, 16)
		readTest(t, dest[:8], tr, 0, 8, false)
		readTest(t, dest[8:16], tr, 8, 8, false)
		readTest(t, dest[16:24], tr, 16, 0, true)
	}

	{
		dest := make([]byte, 32)
		tr := NewTestReader(32, 16)
		readTest(t, dest[:12], tr, 0, 12, false)
		readTest(t, dest[12:24], tr, 12, 4, true)
	}

	{
		dest := make([]byte, 32)
		tr := NewTestReader(32, -1)
		readTest(t, dest[:12], tr, 0, 12, false)
		readTest(t, dest[12:24], tr, 12, 12, false)
		readTest(t, dest[24:32], tr, 24, 8, false)

		n, err := tr.Read(dest)

		if n != 0 || err != io.EOF {
			t.Error("Should have hit EOF")
		}
	}
}

func readTest(t *testing.T, dest []byte, tr *TestReader, min byte, expectedRead int, expectErr bool) {
	n, err := tr.Read(dest)

	if n != expectedRead {
		t.Error("Didn't read expected number of bytes")
	}

	if expectErr && err == nil {
		t.Error("Expected error that didn't happen.")
	} else if !expectErr && err != nil {
		t.Error("Unexpected error.", err)
	}

	for i := 0; i < n; i++ {
		if dest[i] != min+byte(i) {
			t.Error("Unexpected value found at index", i)
		}
	}
}
