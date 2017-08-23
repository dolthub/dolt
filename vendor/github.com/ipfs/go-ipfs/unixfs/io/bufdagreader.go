package io

import (
	"bytes"
	"context"
	"io"
)

type bufDagReader struct {
	*bytes.Reader
}

func NewBufDagReader(b []byte) *bufDagReader {
	return &bufDagReader{bytes.NewReader(b)}
}

var _ DagReader = (*bufDagReader)(nil)

func (*bufDagReader) Close() error {
	return nil
}

func (rd *bufDagReader) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	return rd.Read(b)
}

func (rd *bufDagReader) Offset() int64 {
	of, err := rd.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("this should never happen " + err.Error())
	}
	return of
}

func (rd *bufDagReader) Size() uint64 {
	s := rd.Reader.Size()
	if s < 0 {
		panic("size smaller than 0 (impossible!!)")
	}
	return uint64(s)
}
