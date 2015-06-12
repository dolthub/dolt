package types

import "io"

type Blob interface {
	Value
	Len() uint64
	Reader() io.Reader
}

func NewBlob(data []byte) Blob {
	return flatBlob{data}
}
