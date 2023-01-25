// Copyright 2019 Dolthub, Inc.
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

	nomshash "github.com/dolthub/dolt/go/store/hash"
)

const defaultTableSinkBlockSize = 2 * 1024 * 1024
const defaultChBufferSize = 32 * 1024

// ErrNotFinished is an error returned by a CmpChunkTableWriter when a call to Flush* is called before Finish is called
var ErrNotFinished = errors.New("not finished")

// ErrAlreadyFinished is an error returned if Finish is called more than once on a CmpChunkTableWriter
var ErrAlreadyFinished = errors.New("already Finished")

var ErrChunkAlreadyWritten = errors.New("chunk already written")

// CmpChunkTableWriter writes CompressedChunks to a table file
type CmpChunkTableWriter struct {
	sink                  *HashingByteSink
	totalCompressedData   uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice // TODO: This is in danger of exploding memory
	blockAddr             *addr
	chunkHashes           nomshash.HashSet
	path                  string
}

// NewCmpChunkTableWriter creates a new CmpChunkTableWriter instance with a default ByteSink
func NewCmpChunkTableWriter(tempDir string) (*CmpChunkTableWriter, error) {
	s, err := NewBufferedFileByteSink(tempDir, defaultTableSinkBlockSize, defaultChBufferSize)
	if err != nil {
		return nil, err
	}

	return &CmpChunkTableWriter{NewHashingByteSink(s), 0, 0, nil, nil, nomshash.NewHashSet(), s.path}, nil
}

func (tw *CmpChunkTableWriter) ChunkCount() int {
	return len(tw.prefixes)
}

// Gets the size of the entire table file in bytes
func (tw *CmpChunkTableWriter) ContentLength() uint64 {
	return tw.sink.Size()
}

// Gets the MD5 of the entire table file
func (tw *CmpChunkTableWriter) GetMD5() []byte {
	return tw.sink.GetMD5()
}

// AddCmpChunk adds a compressed chunk
func (tw *CmpChunkTableWriter) AddCmpChunk(c CompressedChunk) error {
	if len(c.CompressedData) == 0 {
		panic("NBS blocks cannot be zero length")
	}

	if tw.chunkHashes.Has(c.H) {
		return ErrChunkAlreadyWritten
	}

	tw.chunkHashes.Insert(c.H)
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
		a.Suffix(),
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

	return tw.sink.FlushToFile(path)
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

func (tw *CmpChunkTableWriter) Reader() (io.ReadCloser, error) {
	if tw.blockAddr == nil {
		return nil, ErrNotFinished
	}
	return tw.sink.Reader()
}

func (tw *CmpChunkTableWriter) Remove() error {
	return os.Remove(tw.path)
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
		n = uint64(copy(buff[offset:], pi.suffix[:]))

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
	err = binary.Write(tw.sink, binary.BigEndian, tw.totalUncompressedData)

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
