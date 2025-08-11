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
	"github.com/edsrzf/mmap-go"

	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
)

const (
	uint64Size = 8
	uint32Size = 4
)

// MmapData holds both the page-aligned mapped region, and the actual requested data range
type MmapData struct {
	data         []byte
	originalData mmap.MMap
}

var mmapAlignment = int64(os.Getpagesize())

func (m MmapData) GetUint64(offset int64) uint64 {
	return binary.BigEndian.Uint64(m.data[offset : offset+uint64Size])
}

func (m MmapData) GetUint32(offset int64) uint32 {
	return binary.BigEndian.Uint32(m.data[offset : offset+uint32Size])
}

func (m *MmapData) ReadAt(p []byte, off int64) (int, error) {
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

func Mmap(file *os.File, offset int64, length int) (reader *MmapData, err error) {
	// Align offset to page boundary
	alignedOffset := offset & ^(mmapAlignment - 1)
	adjustment := offset - alignedOffset
	adjustedLength := length + int(adjustment)

	// Map the region
	mappedData, err := mmap.MapRegion(file, adjustedLength, mmap.RDONLY, 0, alignedOffset)
	if err != nil {
		return &MmapData{}, err
	}

	// Return the adjusted slice starting at the actual offset
	reader = &MmapData{
		data:         mappedData[adjustment : adjustment+int64(length)],
		originalData: mappedData,
	}

	runtime.SetFinalizer(reader, (*MmapData).Close)
	return reader, err
}

func (m *MmapData) Close() error {
	if m.data == nil {
		return nil
	}
	m.data = nil
	originalData := m.originalData
	m.originalData = nil
	return originalData.Unmap()
}
