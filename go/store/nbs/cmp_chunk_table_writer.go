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
	"bufio"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"hash"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

var ErrBufferFull = errors.New("buffer full")

type TableSink interface {
	Write(src []byte) (int, error)
	Flush(path string) error
}

type FixedBufferTableSink struct {
	buff          []byte
	pos           uint64
}

func NewFixedBufferTableSink(buff []byte) *FixedBufferTableSink {
	return &FixedBufferTableSink{buff:buff}
}

func (sink *FixedBufferTableSink) Write(src []byte) (int, error) {
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

func (sink *FixedBufferTableSink) Flush(path string) error {
	return ioutil.WriteFile(path, sink.buff[:sink.pos], os.ModePerm)
}

type BlockBufferTableSink struct {
	blockSize int
	pos 	  uint64
	blocks    [][]byte
}

func NewBlockBufferTableSink(blockSize int) *BlockBufferTableSink {
	block := make([]byte, 0, blockSize)
	return &BlockBufferTableSink{blockSize, 0, [][]byte{block}}
}

func (sink *BlockBufferTableSink) Write(src []byte) (int, error) {
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

func (sink *BlockBufferTableSink) Flush(path string) (err error) {
	var f *os.File
	f, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	if err != nil {
		return err
	}

	defer func() {
		closeErr := f.Close()

		if err == nil {
			err = closeErr
		}
	}()

	err = iohelp.WriteAll(f, sink.blocks...)

	return err
}

type TempFileStreamingSync struct {
	f *os.File
	bufWr *bufio.Writer
	path string
}

func NewTempFileStreamingSync() *TempFileStreamingSync {
	tempPath := filepath.Join(os.TempDir(), uuid.New().String())
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModeTemporary)

	if err != nil {
		panic(err)
	}

	bufWr := bufio.NewWriter(f)

	return &TempFileStreamingSync{f, bufWr, tempPath }
}

func (sink *TempFileStreamingSync) Write(src []byte) (int, error) {
	err := iohelp.WriteAll(sink.bufWr, src)

	if err != nil {
		return 0, err
	}

	return len(src), nil
}

func (sink *TempFileStreamingSync) Flush(path string) error {
	err := sink.bufWr.Flush()

	if err != nil {
		_ = sink.f.Close()
		return err
	}

	err = sink.f.Close()

	if err != nil {
		return err
	}

	err = os.Rename(sink.path, path)

	if err != nil {
		return err
	}

	return nil
}

const defaultTableSinkBlockSize = 2*1024*1024

type CmpChunkTableWriter struct{
	sink 			      TableSink
	totalCompressedData   uint64
	totalUncompressedData uint64
	prefixes              prefixIndexSlice // TODO: This is in danger of exploding memory
}

func NewCmpChunkTableWriter() *CmpChunkTableWriter {
	return &CmpChunkTableWriter{NewBlockBufferTableSink(defaultTableSinkBlockSize), 0, 0, nil}
}

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

func (tw *CmpChunkTableWriter) Finish(outputDir string) (string, error) {
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

	hashStr := blockAddr.String()
	path := filepath.Join(outputDir, hashStr)

	err = tw.sink.Flush(path)

	if err != nil {
		return "", err
	}

	return hashStr, nil
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

