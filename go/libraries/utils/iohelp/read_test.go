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
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/libraries/utils/test"
)

func TestErrPreservingReader(t *testing.T) {
	tr := test.NewTestReader(32, 16)
	epr := NewErrPreservingReader(tr)

	read1, noErr1 := ReadNBytes(epr, 8)
	read2, noErr2 := ReadNBytes(epr, 8)
	read3, firstErr := ReadNBytes(epr, 8)
	read4, secondErr := ReadNBytes(epr, 8)

	for i := 0; i < 8; i++ {
		if read1[i] != byte(i) || read2[i] != byte(i)+8 {
			t.Error("Unexpected values read.")
		}
	}

	if read3 != nil || read4 != nil {
		t.Error("Unexpected read values should be nil.")
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
		unbufferedTest := getTestReadLineNoBufClosure(test.inputStr)

		testReadLineFunctions(t, "buffered", test.expectedLines, bufferedTest)
		testReadLineFunctions(t, "unbuffered", test.expectedLines, unbufferedTest)
	}
}

func getTestReadLineClosure(inputStr string) func() (string, bool, error) {
	r := bytes.NewReader([]byte(inputStr))
	br := bufio.NewReader(r)

	return func() (string, bool, error) {
		return ReadLine(br)
	}
}

func getTestReadLineNoBufClosure(inputStr string) func() (string, bool, error) {
	r := bytes.NewReader([]byte(inputStr))

	return func() (string, bool, error) {
		return ReadLineNoBuf(r)
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

var ErrClosed = errors.New("")

type FixedRateDataGenerator struct {
	BytesPerInterval int
	Interval         time.Duration
	lastRead         time.Time
	closeChan        chan struct{}
	dataGenerated    uint64
}

func NewFixedRateDataGenerator(bytesPerInterval int, interval time.Duration) *FixedRateDataGenerator {
	return &FixedRateDataGenerator{
		bytesPerInterval,
		interval,
		time.Now(),
		make(chan struct{}),
		0,
	}
}

func (gen *FixedRateDataGenerator) Read(p []byte) (int, error) {
	nextRead := gen.Interval - (time.Now().Sub(gen.lastRead))

	select {
	case <-gen.closeChan:
		return 0, ErrClosed
	case <-time.After(nextRead):
		gen.dataGenerated += uint64(gen.BytesPerInterval)
		gen.lastRead = time.Now()
		return min(gen.BytesPerInterval, len(p)), nil
	}
}

func (gen *FixedRateDataGenerator) Close() error {
	close(gen.closeChan)
	return nil
}

type ErroringReader struct {
	Err error
}

func (er ErroringReader) Read(p []byte) (int, error) {
	return 0, er.Err
}

func (er ErroringReader) Close() error {
	return nil
}

type ReaderSizePair struct {
	Reader io.ReadCloser
	Size   int
}

type ReaderCollection struct {
	ReadersAndSizes []ReaderSizePair
	currIdx         int
	currReaderRead  int
}

func NewReaderCollection(readerSizePair ...ReaderSizePair) *ReaderCollection {
	if len(readerSizePair) == 0 {
		panic("no readers")
	}

	for _, rsp := range readerSizePair {
		if rsp.Size <= 0 {
			panic("invalid size")
		}

		if rsp.Reader == nil {
			panic("invalid reader")
		}
	}

	return &ReaderCollection{readerSizePair, 0, 0}
}

func (rc *ReaderCollection) Read(p []byte) (int, error) {
	if rc.currIdx < len(rc.ReadersAndSizes) {
		currReader := rc.ReadersAndSizes[rc.currIdx].Reader
		currSize := rc.ReadersAndSizes[rc.currIdx].Size
		remaining := currSize - rc.currReaderRead

		n, err := currReader.Read(p)

		if err != nil {
			return 0, err
		}

		if n >= remaining {
			n = remaining
			rc.currIdx++
			rc.currReaderRead = 0
		} else {
			rc.currReaderRead += n
		}

		return n, err
	}

	return 0, io.EOF
}

func (rc *ReaderCollection) Close() error {
	for _, rsp := range rc.ReadersAndSizes {
		err := rsp.Reader.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func TestReadWithMinThroughput(t *testing.T) {
	t.Skip("Skipping test in all cases as it is inconsistent on Unix")
	if osutil.IsWindows {
		t.Skip("Skipping test as it is too inconsistent on Windows and will randomly pass or fail")
	}
	tests := []struct {
		name          string
		numBytes      int64
		reader        io.ReadCloser
		mtcp          MinThroughputCheckParams
		expErr        bool
		expThroughErr bool
	}{
		{
			"10MB @ max(100MBps) > 50MBps",
			10 * 1024 * 1024,
			NewReaderCollection(
				ReaderSizePair{NewFixedRateDataGenerator(100*1024, time.Millisecond), 10 * 1024 * 1024},
			),
			MinThroughputCheckParams{50 * 1024 * 1024, 5 * time.Millisecond, 10},
			false,
			false,
		},
		{
			"5MB then error",
			10 * 1024 * 1024,
			NewReaderCollection(
				ReaderSizePair{NewFixedRateDataGenerator(100*1024, time.Millisecond), 5 * 1024 * 1024},
				ReaderSizePair{ErroringReader{errors.New("test err")}, 100 * 1024},
				ReaderSizePair{NewFixedRateDataGenerator(100*1024, time.Millisecond), 5 * 1024 * 1024},
			),
			MinThroughputCheckParams{50 * 1024 * 1024, 5 * time.Millisecond, 10},
			true,
			false,
		},
		{
			"5MB then slow < 50Mbps",
			10 * 1024 * 1024,
			NewReaderCollection(
				ReaderSizePair{NewFixedRateDataGenerator(100*1024, time.Millisecond), 5 * 1024 * 1024},
				ReaderSizePair{NewFixedRateDataGenerator(49*1024, time.Millisecond), 5 * 1024 * 1024},
			),
			MinThroughputCheckParams{50 * 1024 * 1024, 5 * time.Millisecond, 10},
			false,
			true,
		},
		{
			"5MB then stops",
			10 * 1024 * 1024,
			NewReaderCollection(
				ReaderSizePair{NewFixedRateDataGenerator(100*1024, time.Millisecond), 5 * 1024 * 1024},
				ReaderSizePair{NewFixedRateDataGenerator(0, 100*time.Second), 5 * 1024 * 1024},
			),
			MinThroughputCheckParams{50 * 1024 * 1024, 5 * time.Millisecond, 10},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := ReadWithMinThroughput(test.reader, test.numBytes, test.mtcp)

			if test.expErr || test.expThroughErr {
				if test.expThroughErr {
					assert.Equal(t, err, ErrThroughput)
				} else {
					assert.Error(t, err)
					assert.NotEqual(t, err, ErrThroughput)
				}
			} else {
				assert.Equal(t, len(data), int(test.numBytes))
				assert.NoError(t, err)
			}
		})
	}
}
