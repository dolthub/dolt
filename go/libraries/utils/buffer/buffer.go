// Copyright 2021 Dolthub, Inc.
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

package buffer

import (
	"io"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

type DynamicBuffer struct {
	blocks    [][]byte
	blockSize int
}

func New(blockSize int) *DynamicBuffer {
	return &DynamicBuffer{blockSize: blockSize}
}

func (buf *DynamicBuffer) Append(bytes []byte) {
	blockIdx := len(buf.blocks) - 1

	var space int
	var pos int
	if blockIdx >= 0 {
		currBlock := buf.blocks[blockIdx]
		pos = len(currBlock)
		space = cap(currBlock) - pos
	}

	for len(bytes) > 0 {
		if space == 0 {
			for len(bytes) >= buf.blockSize {
				buf.blocks = append(buf.blocks, bytes[:buf.blockSize])
				bytes = bytes[buf.blockSize:]
				blockIdx++
			}

			if len(bytes) == 0 {
				return
			}

			buf.blocks = append(buf.blocks, make([]byte, 0, buf.blockSize))
			pos = 0
			space = buf.blockSize
			blockIdx++
		}

		n := len(bytes)
		if n > space {
			n = space
		}

		buf.blocks[blockIdx] = buf.blocks[blockIdx][:pos+n]
		copy(buf.blocks[blockIdx][pos:], bytes[:n])
		bytes = bytes[n:]
		space -= n
		pos += n
	}
}

func (buf *DynamicBuffer) Close() *BufferIterator {
	itr := &BufferIterator{blocks: buf.blocks}
	buf.blocks = nil

	return itr
}

type BufferIterator struct {
	blocks [][]byte
	i      int
}

func (itr *BufferIterator) Next() ([]byte, error) {
	if itr.i >= len(itr.blocks) {
		return nil, io.EOF
	}
	next := itr.blocks[itr.i]
	itr.i++

	return next, nil
}

func (itr *BufferIterator) NumBlocks() int {
	return len(itr.blocks)
}

func (itr *BufferIterator) FlushTo(wr io.Writer) error {
	for {
		data, err := itr.Next()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		err = iohelp.WriteAll(wr, data)

		if err != nil {
			return err
		}
	}
}

func (itr *BufferIterator) AsReader() io.Reader {
	return &bufferIteratorReader{
		itr: itr,
	}
}

type bufferIteratorReader struct {
	itr      *BufferIterator
	currBuff []byte
}

func (b *bufferIteratorReader) Read(dest []byte) (n int, err error) {
	if len(b.currBuff) == 0 {
		b.currBuff, err = b.itr.Next()

		if err != nil {
			return 0, err
		}
	}

	destSize := len(dest)
	toCopy := b.currBuff
	if len(b.currBuff) > destSize {
		toCopy = b.currBuff[:destSize]
	}

	n = copy(dest, toCopy)
	b.currBuff = b.currBuff[n:]

	return n, err
}
