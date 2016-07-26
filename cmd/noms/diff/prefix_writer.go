// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"
	"io"
)

type prefixWriter struct {
	w      io.Writer
	prefix []byte
	start  bool
}

const (
	ADD = 0
	DEL = 1
)

func newPrefixWriter(w io.Writer, op int) io.Writer {
	var prefix []byte
	switch op {
	case ADD:
		prefix = []byte("+   ")
	case DEL:
		prefix = []byte("-   ")
	default:
		panic("invalid operation")
	}

	return &prefixWriter{w, prefix, true}
}

func (pw *prefixWriter) Write(bs []byte) (n int, err error) {
	writeThrough := func(bs []byte) bool {
		n2, err2 := pw.w.Write(bs)
		n += n2
		err = err2
		return err == nil
	}

	writePrefix := func() bool {
		_, err2 := pw.w.Write(pw.prefix)
		err = err2
		return err == nil
	}

	if pw.start {
		pw.start = false
		if !writePrefix() {
			return
		}
	}

	for len(bs) > 0 {
		idx := bytes.IndexRune(bs, '\n')
		if idx == -1 {
			writeThrough(bs)
			break
		}

		// idx+1 to include the '\n'.
		if !writeThrough(bs[:idx+1]) {
			break
		}

		if !writePrefix() {
			break
		}

		// idx+1 to skip over the '\n'.
		bs = bs[idx+1:]
	}
	return
}
