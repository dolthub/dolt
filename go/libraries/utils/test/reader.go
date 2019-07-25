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

package test

import (
	"io"
	"math"

	"github.com/google/uuid"
)

// TestError is an error that stores a uniquely generated id
type TestError struct {
	ErrId uuid.UUID
}

// NewTestError creates a TestError with a newly created id
func NewTestError() *TestError {
	id, _ := uuid.NewRandom()
	return &TestError{id}
}

// Error is the only method defined in the error interface
func (te *TestError) Error() string {
	return "Error! Error! Error!"
}

// TestReader is an io.Reader that will error after some number of bytes have been read
type TestReader struct {
	data       []byte
	errorAfter int
	totalRead  int
}

// NewTestReader takes a size, and the number of bytes that should be read before the
// reader errors.  The size is used to generate a buffer of random data that will be
// read by the caller.  After the caller has read the specified number of bytes the
// call to read will fail.
func NewTestReader(dataSize, errorAfter int) *TestReader {
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	return NewTestReaderWithData(data, errorAfter)
}

// NewTestReaderWithData creates a TestReader with user supplied data and the number
// of bytes that can be read before errors start occurring on Read calls.
func NewTestReaderWithData(data []byte, errorAfter int) *TestReader {
	return &TestReader{data, errorAfter, 0}
}

// Read reads data from the internal buffer
func (tr *TestReader) Read(p []byte) (int, error) {
	dataRemaining := len(tr.data) - tr.totalRead

	if dataRemaining == 0 {
		return 0, io.EOF
	}

	remainingBeforeErr := math.MaxInt32
	if tr.errorAfter != -1 {
		remainingBeforeErr = tr.errorAfter - tr.totalRead
	}

	toRead := dataRemaining
	if dataRemaining > remainingBeforeErr {
		toRead = remainingBeforeErr
	}

	if toRead > len(p) {
		toRead = len(p)
	}

	var copied int
	if toRead > 0 {
		copied = copy(p[:toRead], tr.data[tr.totalRead:tr.totalRead+toRead])
		tr.totalRead += copied
	}

	var err error
	if tr.totalRead == tr.errorAfter && copied != len(p) {
		err = NewTestError()
	}

	return copied, err
}
