package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/chunks"
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

var typeRefForBlob = MakePrimitiveTypeRef(BlobKind)

type Blob interface {
	Value
	Len() uint64
	// BUG 155 - Should provide Seek and Write... Maybe even have Blob implement ReadWriteSeeker
	Reader() io.ReadSeeker
}

func NewEmptyBlob() Blob {
	return newBlobLeaf([]byte{})
}

func NewMemoryBlob(r io.Reader) (Blob, error) {
	return NewBlob(r, chunks.NewMemoryStore())
}

func NewBlob(r io.Reader, cs chunks.ChunkStore) (Blob, error) {
	length := uint64(0)
	offsets := []uint64{}
	chunks := []ref.Ref{}
	var blob blobLeaf
	for {
		buf := bytes.Buffer{}
		n, err := copyChunk(&buf, r)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if n == 0 {
			// Don't add empty chunk.
			break
		}

		length += n
		offsets = append(offsets, length)
		blob = newBlobLeaf(buf.Bytes())
		chunks = append(chunks, WriteValue(blob, cs))

		if err == io.EOF {
			break
		}
	}

	if length == 0 {
		return newBlobLeaf([]byte{}), nil
	}

	if len(chunks) == 1 {
		return blob, nil
	}

	co := compoundObject{offsets, chunks, &ref.Ref{}, cs}
	co = splitCompoundObject(co, cs)
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
		l, rerr := src.Read(p)
		n += uint64(l)

		// io.Reader can return data and error at the same time, so we need to write before considering the error.
		h.Write(p[:l])
		_, werr := dst.Write(p[:l])

		if rerr != nil {
			return n, rerr
		}

		if werr != nil {
			return n, werr
		}

		if h.Sum32()&blobPattern == blobPattern {
			return n, nil
		}
	}
}
