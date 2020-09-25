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

package iohelp

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/test"
)

type test16ByteWriter struct {
	Data []byte
}

func (tw *test16ByteWriter) Write(p []byte) (n int, err error) {
	toCopy := 16
	if len(p) < toCopy {
		toCopy = len(p)
	}

	tw.Data = append(tw.Data, p[:toCopy]...)

	return toCopy, nil
}

func TestWriteAll(t *testing.T) {
	t16 := &test16ByteWriter{}
	data := test.RandomData(1000)

	err := WriteAll(t16, data)

	if err != nil {
		t.Error("Unexpected error", err)
	}

	if !reflect.DeepEqual(data, t16.Data) {
		t.Error("Failed to write correctly")
	}
}

func TestWriteNoErrWrites(t *testing.T) {
	t16 := &test16ByteWriter{}
	data := test.RandomData(32)

	var prim int32
	err := WritePrimIfNoErr(t16, prim, nil)

	if err != nil {
		t.Error("Unexpected error")
	}

	err = WriteIfNoErr(t16, data, err)

	if err != nil {
		t.Error("Unexpected error")
	}

	sizeAfterSuccesses := len(t16.Data)

	err = errors.New("some error")
	WritePrimIfNoErr(t16, prim, err)

	if err == nil {
		t.Error("Expected error")
	}

	err = WriteIfNoErr(t16, data, err)

	if err == nil {
		t.Error("Expected error")
	}

	if len(t16.Data) != sizeAfterSuccesses {
		t.Error("Should not have written data after err set to non nil.")
	}
}

func TestWriteLine(t *testing.T) {
	lineStr := "This is a test of writing a line."

	t16 := &test16ByteWriter{}
	err := WriteLine(t16, lineStr)

	if err != nil {
		t.Error("Unexpected error", err)
	}

	resultStr := string(t16.Data)
	if resultStr != lineStr+"\n" {
		t.Errorf(`"%s" != "%s"`, resultStr, lineStr)
	}
}
