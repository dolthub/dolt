package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/ref"
)

const (
	// 12 bits leads to an average size of 4k
	// 13 bits leads to an average size of 8k
	// 14 bits leads to an average size of 16k
	blobPattern = uint32(1<<13 - 1)

	// The window size to use for computing the rolling hash.
	blobWindowSize = 64
)

type Blob interface {
	Value
	Len() uint64
	// BUG 155 - Should provide Seek and Write... Maybe even have Blob implement ReadWriteSeeker
	Reader() io.ReadSeeker
}

func NewBlob(r io.Reader) (Blob, error) {
	length := uint64(0)
	offsets := []uint64{}
	blobs := []Future{}
	var blob blobLeaf
	for {
		buf := bytes.Buffer{}
		n, err := copyChunk(&buf, r)
		if err != nil {
			return nil, err
		}

		if n == 0 {
			// Don't add empty chunk.
			break
		}

		length += n
		offsets = append(offsets, length)
		blob = newBlobLeaf(buf.Bytes())
		blobs = append(blobs, futureFromValue(blob))
	}

	if length == 0 {
		return newBlobLeaf([]byte{}), nil
	}

	if len(blobs) == 1 {
		return blob, nil
	}

	co := compoundObject{offsets, blobs, &ref.Ref{}, nil}
	co = splitCompoundObject(co, compoundObjectToBlobFuture)
	return compoundBlob{co}, nil
}

func BlobFromVal(v Value) Blob {
	return v.(Blob)
}

// copyChunk copies from src to dst until a chunk boundary is found.
// It returns the number of bytes copied and the earliest error encountered while copying.
// copyChunk never returns an io.EOF error, instead it returns the number of bytes read up to the io.EOF.
func copyChunk(dst io.Writer, src io.Reader) (n uint64, err error) {
	h := buzhash.NewBuzHash(blobWindowSize)
	p := []byte{0}

	for {
		_, err = src.Read(p)
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return
		}

		h.Write(p)
		_, err = dst.Write(p)
		if err != nil {
			return
		}
		n++

		if h.Sum32()&blobPattern == blobPattern {
			return
		}
	}
}
