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
		cb,
	}
}

type reader struct {
	inner io.Reader
	seen  uint64
	cb    Callback
}

func (r *reader) Read(p []byte) (n int, err error) {
	r.cb(r.seen)
	n, err = r.inner.Read(p)
	r.seen += uint64(n)
	return
}
