// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package encodedbytes

import (
	"bytes"
	"io"
)

// Writer is a helper for writing frame bytes
type Writer struct {
	data  []byte
	index int // current writing index
}

func (w *Writer) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if w.index >= len(w.data) {
		return 0, io.EOF
	}
	n = copy(w.data[w.index:], b)
	w.index += n
	return
}

func (w *Writer) WriteByte(b byte) (err error) {
	if w.index >= len(w.data) {
		return io.EOF
	}
	w.data[w.index] = b
	w.index++
	return
}

func (w *Writer) WriteString(s string, encoding byte) (err error) {
	encodedString, err := Encoders[encoding].ConvertString(s)
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(encodedString))
	if err != nil {
		return err
	}

	return
}

func (w *Writer) WriteNullTermString(s string, encoding byte) (err error) {
	if err = w.WriteString(s, encoding); err != nil {
		return
	}

	nullLength := EncodingNullLengthForIndex(encoding)
	_, err = w.Write(bytes.Repeat([]byte{0x0}, nullLength))

	return
}

func NewWriter(b []byte) *Writer { return &Writer{b, 0} }
