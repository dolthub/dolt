package iface

import (
	"context"
	"io"
)

type Reader interface {
	ReadSeekCloser
	Size() uint64
	CtxReadFull(context.Context, []byte) (int, error)
}

// A ReadSeekCloser implements interfaces to read, copy, seek and close.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
	io.WriterTo
}
