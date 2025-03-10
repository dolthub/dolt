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
	"bytes"
	"crypto/md5"
	"crypto/sha512"
	"errors"
	"hash"
	"io"
	"os"
	"sync"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

func flushSinkToFile(sink ByteSink, path string) (err error) {
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

	err = sink.Flush(f)
	return err
}

// A ByteSink is an interface for writing bytes which can later be flushed to a writer
type ByteSink interface {
	io.Writer

	// Flush writes all the data that was written to the ByteSink to the supplied writer
	Flush(wr io.Writer) error

	// FlushToFile writes all the data that was written to the ByteSink to a file at the given path
	FlushToFile(path string) error

	Reader() (io.ReadCloser, error)
}

// ErrBuffFull used by the FixedBufferSink when the data written is larger than the buffer allocated.
var ErrBufferFull = errors.New("buffer full")

// FixedBufferByteSink is a ByteSink implementation with a buffer whose size will not change.  Writing more
// data than the fixed buffer can hold will result in an error
type FixedBufferByteSink struct {
	buff []byte
	pos  uint64
}

// NewFixedBufferByteSink creates a FixedBufferTableSink which will use the supplied buffer
func NewFixedBufferByteSink(buff []byte) *FixedBufferByteSink {
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

	copy(dest, src)

	sink.pos += uint64(srcLen)
	return srcLen, nil
}

// Flush writes all the data that was written to the ByteSink to the supplied writer
func (sink *FixedBufferByteSink) Flush(wr io.Writer) error {
	return iohelp.WriteAll(wr, sink.buff[:sink.pos])
}

// FlushToFile writes all the data that was written to the ByteSink to a file at the given path
func (sink *FixedBufferByteSink) FlushToFile(path string) (err error) {
	return flushSinkToFile(sink, path)
}

func (sink *FixedBufferByteSink) Reader() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(sink.buff)), nil
}

// BlockBufferByteSink allocates blocks of data with a given block size to store the bytes written to the sink. New
// blocks are allocated as needed in order to handle all the data of the Write calls.
type BlockBufferByteSink struct {
	blockSize int
	pos       uint64
	blocks    [][]byte
}

// NewBlockBufferByteSink creates a BlockBufferByteSink with the provided block size.
func NewBlockBufferByteSink(blockSize int) *BlockBufferByteSink {
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

// FlushToFile writes all the data that was written to the ByteSink to a file at the given path
func (sink *BlockBufferByteSink) FlushToFile(path string) (err error) {
	return flushSinkToFile(sink, path)
}

func (sink *BlockBufferByteSink) Reader() (io.ReadCloser, error) {
	rs := make([]io.Reader, len(sink.blocks))
	for i := range sink.blocks {
		rs[i] = bytes.NewReader(sink.blocks[i])
	}
	return io.NopCloser(io.MultiReader(rs...)), nil
}

// BufferedFileByteSink is a ByteSink implementation that buffers some amount of data before it passes it
// to a background writing thread to be flushed to a file.
type BufferedFileByteSink struct {
	blockSize    int
	pos          uint64
	currentBlock []byte

	writeCh chan []byte
	ae      *atomicerr.AtomicError
	wg      *sync.WaitGroup

	wr   io.WriteCloser
	path string
}

// NewBufferedFileByteSink creates a BufferedFileByteSink
func NewBufferedFileByteSink(tempDir string, blockSize, chBufferSize int) (*BufferedFileByteSink, error) {
	f, err := tempfiles.MovableTempFileProvider.NewFile(tempDir, "buffered_file_byte_sink_")

	if err != nil {
		return nil, err
	}

	sink := &BufferedFileByteSink{
		blockSize:    blockSize,
		currentBlock: make([]byte, blockSize),
		writeCh:      make(chan []byte, chBufferSize),
		ae:           atomicerr.New(),
		wg:           &sync.WaitGroup{},
		wr:           f,
		path:         f.Name(),
	}

	sink.wg.Add(1)
	go func() {
		defer sink.wg.Done()
		sink.backgroundWrite()
	}()

	return sink, nil
}

// Write writes a byte array to the sink.
func (sink *BufferedFileByteSink) Write(src []byte) (int, error) {
	srcLen := len(src)
	remaining := cap(sink.currentBlock) - len(sink.currentBlock)

	if remaining >= srcLen {
		sink.currentBlock = append(sink.currentBlock, src...)

		if remaining == srcLen {
			sink.writeCh <- sink.currentBlock
			sink.currentBlock = nil
		}
	} else {
		if remaining > 0 {
			sink.currentBlock = append(sink.currentBlock, src[:remaining]...)
			sink.writeCh <- sink.currentBlock
		}

		newBlock := make([]byte, 0, sink.blockSize)
		newBlock = append(newBlock, src[remaining:]...)
		sink.currentBlock = newBlock
	}

	sink.pos += uint64(srcLen)
	return srcLen, nil
}

func (sink *BufferedFileByteSink) backgroundWrite() {
	var err error
	for buff := range sink.writeCh {
		if err != nil {
			continue // drain
		}

		err = iohelp.WriteAll(sink.wr, buff)
		sink.ae.SetIfError(err)
	}

	err = sink.wr.Close()
	sink.ae.SetIfError(err)
}

func (sink *BufferedFileByteSink) finish() error {
	// |finish()| is not thread-safe. We just use writeCh == nil as a
	// sentinel to mean we've been called again from Reader() as part of a
	// retry or something.
	if sink.writeCh != nil {
		toWrite := len(sink.currentBlock)
		if toWrite > 0 {
			sink.writeCh <- sink.currentBlock[:toWrite]
		}

		close(sink.writeCh)
		sink.wg.Wait()

		sink.writeCh = nil
	}
	return sink.ae.Get()
}

// Flush writes all the data that was written to the ByteSink to the supplied writer
func (sink *BufferedFileByteSink) Flush(wr io.Writer) (err error) {
	err = sink.finish()
	if err != nil {
		return err
	}

	var f *os.File
	f, err = os.Open(sink.path)

	if err != nil {
		return err
	}

	defer func() {
		closeErr := f.Close()

		if err == nil {
			err = closeErr
		}
	}()

	_, err = io.Copy(wr, f)

	return err
}

// FlushToFile writes all the data that was written to the ByteSink to a file at the given path
func (sink *BufferedFileByteSink) FlushToFile(path string) (err error) {
	err = sink.finish()
	if err != nil {
		return err
	}

	return file.Rename(sink.path, path)
}

func (sink *BufferedFileByteSink) Reader() (io.ReadCloser, error) {
	err := sink.finish()
	if err != nil {
		return nil, err
	}
	return os.Open(sink.path)
}

// HashingByteSink is a ByteSink that keeps an hash of all the data written to it.
type HashingByteSink struct {
	backingSink ByteSink
	hasher      hash.Hash
	size        uint64
}

func NewSHA512HashingByteSink(backingSink ByteSink) *HashingByteSink {
	return &HashingByteSink{backingSink: backingSink, hasher: sha512.New(), size: 0}
}

func NewMD5HashingByteSink(backingSink ByteSink) *HashingByteSink {
	return &HashingByteSink{backingSink: backingSink, hasher: md5.New(), size: 0}
}

// Write writes a byte array to the sink.
func (sink *HashingByteSink) Write(src []byte) (int, error) {
	nWritten, err := sink.backingSink.Write(src)

	if err != nil {
		return 0, err
	}

	nHashed, err := sink.hasher.Write(src[:nWritten])

	if err != nil {
		return 0, err
	} else if nWritten != nHashed {
		return 0, errors.New("failed to hash all the data that was written to the byte sink.")
	}

	sink.size += uint64(nWritten)

	return nWritten, nil
}

// Flush writes all the data that was written to the ByteSink to the supplied writer
func (sink *HashingByteSink) Flush(wr io.Writer) error {
	return sink.backingSink.Flush(wr)
}

// FlushToFile writes all the data that was written to the ByteSink to a file at the given path
func (sink *HashingByteSink) FlushToFile(path string) error {
	return sink.backingSink.FlushToFile(path)
}

func (sink *HashingByteSink) Reader() (io.ReadCloser, error) {
	return sink.backingSink.Reader()
}

// Execute the hasher.Sum() function and return the result
func (sink *HashingByteSink) GetSum() []byte {
	return sink.hasher.Sum(nil)
}

// ResetHasher resets the hasher to allow for checksums at various points in the data stream. The expectation is that
// you would call GetSum prior to calling this function.
func (sink *HashingByteSink) ResetHasher() {
	sink.hasher.Reset()
}

// Size gets the number of bytes written to the sink
func (sink *HashingByteSink) Size() uint64 {
	return sink.size
}
