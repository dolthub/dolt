// Copyright 2025 Dolthub, Inc.
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

package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	uint64Size = 8
	uint32Size = 4
)

type FileReaderAt interface {
	io.ReaderAt
	io.Closer
	GetUint64(offset int64) uint64
	GetUint32(offset int64) uint32
}

var mmapAlignment = int64(os.Getpagesize())

func (m mmapData) GetUint64(offset int64) uint64 {
	return binary.BigEndian.Uint64(m.data[offset : offset+uint64Size])
}

func (m mmapData) GetUint32(offset int64) uint32 {
	return binary.BigEndian.Uint32(m.data[offset : offset+uint32Size])
}

func (m *mmapData) ReadAt(p []byte, off int64) (int, error) {
	if m.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || int64(len(m.data)) < off {
		return 0, fmt.Errorf("mmap: invalid ReadAt offset %d", off)
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
