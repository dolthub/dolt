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
	prefix prefixOp
	start  bool
}

type prefixOp string

const (
	ADD prefixOp = "+   "
	DEL prefixOp = "-   "
)

func newPrefixWriter(w io.Writer, op prefixOp) io.Writer {
	return &prefixWriter{w, op, true}
}

func (pw *prefixWriter) Write(bs []byte) (n int, err error) {
	writeThrough := func(bs []byte) bool {
		n2, err2 := pw.w.Write(bs)
		n += n2
		err = err2
		return err == nil
	}

	writePrefix := func() bool {
		_, err2 := pw.w.Write([]byte(pw.prefix))
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
