// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package progressreader provides an io.Reader that reports progress to a callback
package progressreader

import (
	"io"
)

type Callback func(seen uint64)

func New(inner io.Reader, cb Callback) io.Reader {
	return &reader{
		inner,
		uint64(0),
		uint64(0),
		cb,
	}
}

type reader struct {
	inner    io.Reader
	seen     uint64
	lastMult uint64
	cb       Callback
}

const reportFreq = 2 << 19 // 1 MB

func (r *reader) Read(p []byte) (n int, err error) {
	mult := uint64(r.seen / reportFreq)
	if mult > r.lastMult {
		r.cb(r.seen)
		r.lastMult = mult
	}

	n, err = r.inner.Read(p)
	r.seen += uint64(n)
	return
}
