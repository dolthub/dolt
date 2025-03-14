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
	"fmt"
	gohash "hash"
	"io"
	"os"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/golang/snappy"
)

// GenericTableWriter is an interface for writing table files regardless of the output format
type GenericTableWriter interface {
	// Reader returns a reader for the table file as a stream.
	Reader() (io.ReadCloser, error)
	// Finish completed the writing of the table file and returns the calculated name of the table. Note that Finish
	// doesn't move the file, but it returns the name that the file should be moved to.
	// It also returns the additional bytes written to the table file. Those bytes are included in the ContentLength.
	Finish() (uint32, string, error)
	// ChunkCount returns the number of chunks written to the table file. This can be called before Finish to determine
	// if the maximum number of chunks has been reached.
	ChunkCount() int
	// AddChunk adds a chunk to the table file. The underlying implementation of ToChunker will probably be exploited
	// by implementors of GenericTableWriter so that their bytes can be efficiently written to the table file.
	//
	// The number of bytes written to storage is returned. This could be 0 even on success if the writer decides
	// to defer writing the chunks. In the event that AddChunk triggers a flush, the number of bytes written to storage
	// will be returned.
	//
	// If no error occurs, the number of bytes written to the store is returned.
	AddChunk(ToChunker) (uint32, error)
	// ChunkDataLentgh returns the number of bytes written which are specifically tracked data. It will not include
	// data written for the indexes of the storage files. The returned value is only valid after Finish is called.
	ChunkDataLength() (uint64, error)
	// FullLength returns the number of bytes written to the table file.
	FullLength() uint64
	// GetMD5 returns the MD5 hash of the table file. This can can only be called after Finish.
	GetMD5() []byte
	// Remove cleans up and artifacts created by the table writer. Called after everything else is done.
	Remove() error
}

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
	chunkDataLength       uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice
	blockAddr             *hash.Hash
	path                  string
}

var _ GenericTableWriter = (*CmpChunkTableWriter)(nil)

// NewCmpChunkTableWriter creates a new CmpChunkTableWriter instance with a default ByteSink
func NewCmpChunkTableWriter(tempDir string) (*CmpChunkTableWriter, error) {
	s, err := NewBufferedFileByteSink(tempDir, defaultTableSinkBlockSize, defaultChBufferSize)
	if err != nil {
		return nil, err
	}

	return &CmpChunkTableWriter{
		sink:                  NewMD5HashingByteSink(s),
		chunkDataLength:       0,
		totalUncompressedData: 0,
		prefixes:              nil,
		blockAddr:             nil,
		path:                  s.path,
	}, nil
}

func (tw *CmpChunkTableWriter) ChunkCount() int {
	return len(tw.prefixes)
}

// Gets the size of the entire table file in bytes
func (tw *CmpChunkTableWriter) FullLength() uint64 {
	return tw.sink.Size()
}

// Gets the MD5 of the entire table file
func (tw *CmpChunkTableWriter) GetMD5() []byte {
	return tw.sink.GetSum()
}

// AddCmpChunk adds a compressed chunk
func (tw *CmpChunkTableWriter) AddChunk(tc ToChunker) (uint32, error) {
	if tc.IsGhost() {
		// Ghost chunks cannot be written to a table file. They should
		// always be filtered by the write processes before landing
		// here.
		return 0, ErrGhostChunkRequested
	}
	if tc.IsEmpty() {
		panic("NBS blocks cannot be zero length")
	}

	c, ok := tc.(CompressedChunk)
	if !ok {
		if arc, ok := tc.(ArchiveToChunker); ok {
			// Decompress, and recompress since we can only write snappy compressed objects to this store.
			chk, err := arc.ToChunk()
			if err != nil {
				return 0, err
			}
			c = ChunkToCompressedChunk(chk)
		} else {
			panic(fmt.Sprintf("Unknown chunk type: %T", tc))
		}
	}

	uncmpLen, err := snappy.DecodedLen(c.CompressedData)

	if err != nil {
		return 0, err
	}

	fullLen := uint32(len(c.FullCompressedChunk))
	_, err = tw.sink.Write(c.FullCompressedChunk)

	if err != nil {
		return 0, err
	}

	tw.totalUncompressedData += uint64(uncmpLen)

	// Stored in insertion order
	tw.prefixes = append(tw.prefixes, prefixIndexRec{
		c.H,
		uint32(len(tw.prefixes)),
		fullLen,
	})

	return fullLen, nil
}

// Finish will write the index and footer of the table file and return the id of the file.
func (tw *CmpChunkTableWriter) Finish() (uint32, string, error) {
	if tw.blockAddr != nil {
		return 0, "", ErrAlreadyFinished
	}

	startSize := tw.sink.Size()
	// This happens to be the chunk data size.
	tw.chunkDataLength = startSize

	blockHash, err := tw.writeIndex()

	if err != nil {
		return 0, "", err
	}

	err = tw.writeFooter()

	if err != nil {
		return 0, "", err
	}

	var h []byte
	h = blockHash.Sum(h)
	blockAddr := hash.New(h[:hash.ByteLen])

	tw.blockAddr = &blockAddr

	endSize := tw.sink.Size()
	return uint32(endSize - startSize), tw.blockAddr.String(), nil
}

func (tw *CmpChunkTableWriter) ChunkDataLength() (uint64, error) {
	if tw.chunkDataLength == 0 {
		return 0, errors.New("runtime error: ChunkDataLength invalid before Finish")
	}

	return tw.chunkDataLength, nil
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
