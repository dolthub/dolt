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
	gohash "hash"
	"io"
	"os"
	"sort"

	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/hash"
)

const defaultTableSinkBlockSize = 2 * 1024 * 1024
const defaultChBufferSize = 32 * 1024

// ErrNotFinished is an error returned by a CmpChunkTableWriter when a call to Flush* is called before Finish is called
var ErrNotFinished = errors.New("not finished")

// ErrAlreadyFinished is an error returned if Finish is called more than once on a CmpChunkTableWriter
var ErrAlreadyFinished = errors.New("already Finished")

// ErrDuplicateChunkWritten is returned by Finish if the same chunk was given to the writer multiple times.
var ErrDuplicateChunkWritten = errors.New("duplicate chunks written")

// CmpChunkTableWriter writes CompressedChunks to a table file
type CmpChunkTableWriter struct {
	sink                  *HashingByteSink
	totalCompressedData   uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice
	blockAddr             *hash.Hash
	path                  string
}

// NewCmpChunkTableWriter creates a new CmpChunkTableWriter instance with a default ByteSink
func NewCmpChunkTableWriter(tempDir string) (*CmpChunkTableWriter, error) {
	s, err := NewBufferedFileByteSink(tempDir, defaultTableSinkBlockSize, defaultChBufferSize)
	if err != nil {
		return nil, err
	}

	return &CmpChunkTableWriter{NewMD5HashingByteSink(s), 0, 0, nil, nil, s.path}, nil
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
	return tw.sink.GetSum()
}

// AddCmpChunk adds a compressed chunk
func (tw *CmpChunkTableWriter) AddCmpChunk(c CompressedChunk) error {
	if c.IsGhost() {
		// Ghost chunks cannot be written to a table file. They should
		// always be filtered by the write processes before landing
		// here.
		return ErrGhostChunkRequested
	}
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

	// Stored in insertion order
	tw.prefixes = append(tw.prefixes, prefixIndexRec{
		c.H,
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
	blockAddr := hash.New(h[:hash.ByteLen])

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

// Cancel the inprogress write and attempt to cleanup any
// resources associated with it. It is an error to call
// Flush{,ToFile} or Reader after canceling the writer.
func (tw *CmpChunkTableWriter) Cancel() error {
	closer, err := tw.sink.Reader()
	if err != nil {
		return err
	}
	err = closer.Close()
	if err != nil {
		return err
	}
	return tw.Remove()
}

func containsDuplicates(prefixes prefixIndexSlice) bool {
	if len(prefixes) == 0 {
		return false
	}
	for i := 0; i < len(prefixes); i++ {
		curr := prefixes[i]
		// The list is sorted by prefixes. We have to perform n^2
		// checks against every run of matching prefixes. For all
		// shapes of real world data this is not a concern.
		for j := i + 1; j < len(prefixes); j++ {
			cmp := prefixes[j]
			if cmp.addr.Prefix() != curr.addr.Prefix() {
				break
			}
			if cmp.addr == curr.addr {
				return true
			}
		}
	}
	return false
}

func (tw *CmpChunkTableWriter) writeIndex() (gohash.Hash, error) {
	sort.Sort(tw.prefixes)

	// We do a sanity check here to assert that we are never writing duplicate chunks into
	// a table file using this interface.
	if containsDuplicates(tw.prefixes) {
		return nil, ErrDuplicateChunkWritten
	}

	pfxScratch := [hash.PrefixLen]byte{}
	blockHash := sha512.New()

	numRecords := uint32(len(tw.prefixes))
	lengthsOffset := lengthsOffset(numRecords)   // skip prefix and ordinal for each record
	suffixesOffset := suffixesOffset(numRecords) // skip size for each record
	suffixesLen := uint64(numRecords) * hash.SuffixLen
	buff := make([]byte, suffixesLen+suffixesOffset)

	var pos uint64
	for _, pi := range tw.prefixes {
		binary.BigEndian.PutUint64(pfxScratch[:], pi.addr.Prefix())

		// hash prefix
		n := uint64(copy(buff[pos:], pfxScratch[:]))
		if n != hash.PrefixLen {
			return nil, errors.New("failed to copy all data")
		}
		pos += hash.PrefixLen

		// order
		binary.BigEndian.PutUint32(buff[pos:], pi.order)
		pos += ordinalSize

		// length
		offset := lengthsOffset + uint64(pi.order)*lengthSize
		binary.BigEndian.PutUint32(buff[offset:], pi.size)

		// hash suffix
		offset = suffixesOffset + uint64(pi.order)*hash.SuffixLen
		n = uint64(copy(buff[offset:], pi.addr.Suffix()))

		if n != hash.SuffixLen {
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
