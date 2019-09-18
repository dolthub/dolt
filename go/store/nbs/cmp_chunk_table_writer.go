// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"os"
	"sort"

	"github.com/golang/snappy"

	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

// A ByteSink is an interface for writing bytes which can later be flushed to a writer
type ByteSink interface {
	io.Writer

	// Flush writes all the data that was written to the ByteSink to the supplied writer
	Flush(wr io.Writer) error
}

// ErrBuffFull used by the FixedBufferSink when the data written is larger than the buffer allocated.
var ErrBufferFull = errors.New("buffer full")

// FixedBufferByteSink is a ByteSink implementation with a buffer whose size will not change.  Writing more
// data than the fixed buffer can hold will result in an error
type FixedBufferByteSink struct {
	buff []byte
	pos  uint64
}

// NewFixedBufferTableSink creates a FixedBufferTableSink which will use the supplied buffer
func NewFixedBufferTableSink(buff []byte) *FixedBufferByteSink {
	if len(buff) == 0 {
		panic("must provide a buffer")
	}

	return &FixedBufferByteSink{buff: buff}
}

// Write writes a byte array to the sink.
func (sink *FixedBufferByteSink) Write(src []byte) (int, error) {
	dest := sink.buff[sink.pos:]
	destLen := len(dest)
	srcLen := len(src)

	if destLen < srcLen {
		return 0, ErrBufferFull
	}

	n := copy(dest, src)

	if n != srcLen {
		return 0, ErrBufferFull
	}

	sink.pos += uint64(n)
	return srcLen, nil
}

// Flush writes all the data that was written to the ByteSink to the supplied writer
func (sink *FixedBufferByteSink) Flush(wr io.Writer) error {
	return iohelp.WriteAll(wr, sink.buff[:sink.pos])
}

// BlockBufferByteSink allocates blocks of data which of a given block size to store the bytes written to the sink. New
// blocks are allocated as needed in order to handle all the data of the Write calls.
type BlockBufferByteSink struct {
	blockSize int
	pos       uint64
	blocks    [][]byte
}

// NewBlockBufferTableSink creates a BlockBufferByteSink with the provided block size.
func NewBlockBufferTableSink(blockSize int) *BlockBufferByteSink {
	block := make([]byte, 0, blockSize)
	return &BlockBufferByteSink{blockSize, 0, [][]byte{block}}
}

// Write writes a byte array to the sink.
func (sink *BlockBufferByteSink) Write(src []byte) (int, error) {
	srcLen := len(src)
	currBlockIdx := len(sink.blocks) - 1
	currBlock := sink.blocks[currBlockIdx]
	remaining := cap(currBlock) - len(currBlock)

	if remaining >= srcLen {
		currBlock = append(currBlock, src...)
		sink.blocks[currBlockIdx] = currBlock
	} else {
		if remaining > 0 {
			currBlock = append(currBlock, src[:remaining]...)
			sink.blocks[currBlockIdx] = currBlock
		}

		newBlock := make([]byte, 0, sink.blockSize)
		newBlock = append(newBlock, src[remaining:]...)
		sink.blocks = append(sink.blocks, newBlock)
	}

	sink.pos += uint64(srcLen)
	return srcLen, nil
}

// Flush writes all the data that was written to the ByteSink to the supplied writer
func (sink *BlockBufferByteSink) Flush(wr io.Writer) (err error) {
	return iohelp.WriteAll(wr, sink.blocks...)
}

const defaultTableSinkBlockSize = 2 * 1024 * 1024

// ErrNotFinished is an error returned by a CmpChunkTableWriter when a call to Flush* is called before Finish is called
var ErrNotFinished = errors.New("Not finished")

// ErrAlreadyFinished is an error returned if Finish is called more than once on a CmpChunkTableWriter
var ErrAlreadyFinished = errors.New("Already Finished")

// CmpChunkTableWriter writes CompressedChunks to a table file
type CmpChunkTableWriter struct {
	sink                  ByteSink
	totalCompressedData   uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice // TODO: This is in danger of exploding memory
	blockAddr             *addr
}

// NewCmpChunkTableWriter creates a new CmpChunkTableWriter instance with a default ByteSink
func NewCmpChunkTableWriter() *CmpChunkTableWriter {
	return &CmpChunkTableWriter{NewBlockBufferTableSink(defaultTableSinkBlockSize), 0, 0, nil, nil}
}

// AddCmpChunk adds a compressed chunk
func (tw *CmpChunkTableWriter) AddCmpChunk(c CompressedChunk) error {
	if len(c.CompressedData) == 0 {
		panic("NBS blocks cannot be zero length")
	}

	uncmpLen, err := snappy.DecodedLen(c.CompressedData)

	if err != nil {
		return err
	}

	fullLen := len(c.FullCompressedChunk)
	_, err = tw.sink.Write(c.FullCompressedChunk)

	if err != nil {
		return err
	}

	tw.totalCompressedData += uint64(len(c.CompressedData))
	tw.totalUncompressedData += uint64(uncmpLen)

	a := addr(c.H)
	// Stored in insertion order
	tw.prefixes = append(tw.prefixes, prefixIndexRec{
		a.Prefix(),
		a[addrPrefixSize:],
		uint32(len(tw.prefixes)),
		uint32(fullLen),
	})

	return nil
}

// Finish will write the index and footer of the table file and return the id of the file.
func (tw *CmpChunkTableWriter) Finish() (string, error) {
	if tw.blockAddr != nil {
		return "", ErrAlreadyFinished
	}

	blockHash, err := tw.writeIndex()

	if err != nil {
		return "", err
	}

	err = tw.writeFooter()

	if err != nil {
		return "", err
	}

	var h []byte
	h = blockHash.Sum(h)

	var blockAddr addr
	copy(blockAddr[:], h)

	tw.blockAddr = &blockAddr
	return tw.blockAddr.String(), nil
}

// FlushToFile can be called after Finish in order to write the data out to the path provided.
func (tw *CmpChunkTableWriter) FlushToFile(path string) error {
	if tw.blockAddr == nil {
		return ErrNotFinished
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	if err != nil {
		return err
	}

	err = tw.sink.Flush(f)

	if err != nil {
		return err
	}

	return nil
}

// Flush can be called after Finish in order to write the data out to the writer provided.
func (tw *CmpChunkTableWriter) Flush(wr io.Writer) error {
	if tw.blockAddr == nil {
		return ErrNotFinished
	}

	err := tw.sink.Flush(wr)

	if err != nil {
		return err
	}

	return nil
}

func (tw *CmpChunkTableWriter) writeIndex() (hash.Hash, error) {
	sort.Sort(tw.prefixes)

	pfxScratch := [addrPrefixSize]byte{}
	blockHash := sha512.New()

	numRecords := uint32(len(tw.prefixes))
	lengthsOffset := lengthsOffset(numRecords)   // skip prefix and ordinal for each record
	suffixesOffset := suffixesOffset(numRecords) // skip size for each record
	suffixesLen := uint64(numRecords) * addrSuffixSize
	buff := make([]byte, suffixesLen+suffixesOffset)

	var pos uint64
	for _, pi := range tw.prefixes {
		binary.BigEndian.PutUint64(pfxScratch[:], pi.prefix)

		// hash prefix
		n := uint64(copy(buff[pos:], pfxScratch[:]))
		if n != addrPrefixSize {
			return nil, errors.New("failed to copy all data")
		}

		pos += n

		// order
		binary.BigEndian.PutUint32(buff[pos:], pi.order)
		pos += ordinalSize

		// length
		offset := lengthsOffset + uint64(pi.order)*lengthSize
		binary.BigEndian.PutUint32(buff[offset:], pi.size)

		// hash suffix
		offset = suffixesOffset + uint64(pi.order)*addrSuffixSize
		n = uint64(copy(buff[offset:], pi.suffix))

		if n != addrSuffixSize {
			return nil, errors.New("failed to copy all bytes")
		}
	}

	blockHash.Write(buff[suffixesOffset:])
	_, err := tw.sink.Write(buff)

	if err != nil {
		return nil, err
	}

	return blockHash, nil
}

func (tw *CmpChunkTableWriter) writeFooter() error {
	// chunk count
	err := binary.Write(tw.sink, binary.BigEndian, uint32(len(tw.prefixes)))

	if err != nil {
		return err
	}

	// total uncompressed chunk data
	err = binary.Write(tw.sink, binary.BigEndian, uint64(tw.totalUncompressedData))

	if err != nil {
		return err
	}

	// magic number
	_, err = tw.sink.Write([]byte(magicNumber))

	if err != nil {
		return err
	}

	return nil
}
