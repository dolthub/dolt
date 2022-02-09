package nbs

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrNotEnoughBytes = errors.New("reader did not return enough bytes")
)

func NewIndexTransformer(src io.Reader, chunkCount int) io.Reader {
	tuplesSize := chunkCount * prefixTupleSize
	lengthsSize := chunkCount * lengthSize
	suffixesSize := chunkCount * addrSuffixSize

	tupleReader := io.LimitReader(src, int64(tuplesSize))
	lengthsReader := io.LimitReader(src, int64(lengthsSize))
	suffixesReader := io.LimitReader(src, int64(suffixesSize))

	return io.MultiReader(
		tupleReader,
		NewOffsetsReader(lengthsReader),
		suffixesReader,
	)
}

// OffsetsReader transforms a byte stream of table file lengths
// into a byte stream of table file offsets
type OffsetsReader struct {
	lengthsReader io.Reader
	offset        uint64
}

func NewOffsetsReader(lengthsReader io.Reader) *OffsetsReader {
	return &OffsetsReader{
		lengthsReader: lengthsReader,
	}
}

func (tra *OffsetsReader) Read(p []byte) (n int, err error) {
	// if len(p) < offsetSize {
	// 	// ASK: Should this be a panic?
	// 	// If this case is true, 0 bytes will be read and no error will be
	// 	// returned which is undesirable behavior for io.Reader

	// 	// We could return an error instead, but this feels like developer error
	// 	panic("len(p) must be at-least offsetSize")
	// }

	// Read as many lengths, as offsets we can fit into p. Which is half.
	// Below assumes that lengthSize * 2 = offsetSize

	// Strategy is to first read lengths into the second half of p
	// Then, while iterating the lengths, compute the current offset,
	// and write it to the beginning of p.

	// Align p
	rem := len(p) % offsetSize
	p = p[:len(p)-rem]

	// Read lengths into second half of p
	secondHalf := p[len(p)/2:]
	n, err = tra.lengthsReader.Read(secondHalf)
	if err != nil {
		return 0, err
	}
	if n%lengthSize != 0 {
		return 0, ErrNotEnoughBytes
	}

	// Iterate lengths in second half of p while writing offsets starting from the beginning.
	// On the last iteration, we overwrite the last length with the final offset.
	for l, r := 0, 0; r < n; l, r = l+offsetSize, r+lengthSize {
		lengthBytes := secondHalf[r : r+lengthSize]
		length := binary.BigEndian.Uint32(lengthBytes)
		tra.offset += uint64(length)

		offsetBytes := p[l : l+offsetSize]
		binary.BigEndian.PutUint64(offsetBytes, tra.offset)
	}

	return n * 2, nil
}
