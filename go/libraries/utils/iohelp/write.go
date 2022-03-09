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
	"encoding/binary"
	"io"
)

// WriteIfNoErr allows you to chain calls to write and handle errors at the very end.  If the error passed into the
// the function is non-nil then the error will be returned without any io.  If the error is nil then all bytes in the
// supplied buffer will be written to the io.Writer
func WriteIfNoErr(w io.Writer, bytes []byte, err error) error {
	if err != nil {
		return err
	}

	return WriteAll(w, bytes)
}

// WritePrimIfNoErr allows you to chain calls to write and handle errors at the very end.  If the error passed into the
// function is non-nil then the error will be returned without any io.  If the error is nil then supplied primitive will
// be written to the io.Writer using binary.Write with BigEndian byte ordering
func WritePrimIfNoErr(w io.Writer, prim interface{}, err error) error {
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, prim)
}

// WriteAll will write the entirety of the supplied data buffers to an io.Writer.  This my result in multiple calls to
// the io.Writer's Write method in order to write the entire buffer, and if at any point there is an error then
// the error will be returned.
func WriteAll(w io.Writer, dataBuffers ...[]byte) error {
	for _, data := range dataBuffers {
		dataSize := len(data)
		for written := 0; written < dataSize; {
			n, err := w.Write(data[written:])

			if err != nil {
				return err
			}

			written += n
		}
	}

	return nil
}

var newLineBuf = []byte("\n")

// WriteLine will write the given string to an io.Writer followed by a newline.
func WriteLine(w io.Writer, line string) error {
	return WriteAll(w, []byte(line), newLineBuf)
}

// WriteWithoutNewLine will write the given string to an io.Writer followed by nothing.
func WriteWithoutNewLine(w io.Writer, line string) error {
	return WriteAll(w, []byte(line), []byte{})
}

// WriteLines will write the given strings to an io.Writer, each followed by a newline.
func WriteLines(w io.Writer, lines ...string) error {
	for _, line := range lines {
		err := WriteLine(w, line)
		if err != nil {
			return err
		}
	}
	return nil
}

type nopWrCloser struct {
	io.Writer
}

func (nopWrCloser) Close() error { return nil }

// NopWrCloser returns a WriteCloser with a no-op Close method wrapping the provided Writer wr.
func NopWrCloser(wr io.Writer) io.WriteCloser {
	return nopWrCloser{wr}
}
