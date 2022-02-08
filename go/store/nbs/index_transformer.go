package nbs

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrNotEnoughBytes = errors.New("reader did not return enough bytes")
)

// IndexTransformer transforms a table file index byte stream with lengths
// to a table file index byte stream with offsets
type IndexTransformer struct {
	src io.Reader

	lengthsIdx  int64 // Start index of lengths in table file byte stream
	suffixesIdx int64 // Start index of suffixes in table file byte stream
	buff        []byte
	idx         int64
	offset      uint64
}

// Create an IndexTransform given a src reader, chunkCount, and maximum size of read
func NewIndexTransformer(src io.Reader, chunkCount int, maxReadSize int) *IndexTransformer {
	tuplesSize := int64(chunkCount) * prefixTupleSize
	lengthsSize := int64(chunkCount) * lengthSize

	maxNumOffsetsToRead := maxReadSize / offsetSize
	buffSize := maxNumOffsetsToRead * lengthSize

	return &IndexTransformer{
		src:         src,
		buff:        make([]byte, buffSize),
		lengthsIdx:  tuplesSize,
		suffixesIdx: tuplesSize + lengthsSize,
	}
}

func (tra *IndexTransformer) Read(p []byte) (n int, err error) {
	// If we will read outside of lengths, just read.
	if tra.idx+int64(len(p)) < tra.lengthsIdx || tra.idx >= tra.suffixesIdx {
		n, err = tra.src.Read(p)
		tra.idx += int64(n)
		return n, err
	}

	// If we will read on the boundary between tuples and lengths,
	// read up to the start of the lengths.
	if tra.idx < tra.lengthsIdx {
		b := p[:tra.lengthsIdx-tra.idx]
		n, err := tra.src.Read(b)
		tra.idx += int64(n)
		return n, err
	}

	if len(p) < offsetSize {
		// ASK: Should this be a panic?
		// If this case is true, 0 bytes will be read and no error will be
		// returned which is undesirable behavior for io.Reader

		// We could return an error instead, but this feels like developer error
		panic("len(p) must be at-least offsetSize")
	}

	// Now we can assume we are on a length boundary.

	// Alter size of p so we don't read any suffix bytes
	if int64(len(p)) > tra.idx-tra.suffixesIdx {
		p = p[tra.idx-tra.suffixesIdx:]
	}

	// Read as many lengths, as offsets we can fit into p. (Assuming lengthsSize < offsetSize)

	num := n / offsetSize
	readSize := num * lengthSize

	b := p[readSize:]
	n, err = tra.src.Read(b)
	if err != nil {
		return n, err
	}
	if n != readSize {
		return n, ErrNotEnoughBytes
	}
	tra.idx += int64(n)

	// Copy lengths
	copy(tra.buff, b)

	// Calculate offsets
	for lStart, oStart := 0, 0; lStart < readSize; lStart, oStart = lStart+lengthSize, oStart+offsetSize {
		lengthBytes := tra.buff[lStart : lStart+lengthSize]
		length := binary.BigEndian.Uint32(lengthBytes)
		tra.offset += uint64(length)
		binary.BigEndian.PutUint64(p[oStart:oStart+offsetSize], tra.offset)
	}

	return n, nil
}
