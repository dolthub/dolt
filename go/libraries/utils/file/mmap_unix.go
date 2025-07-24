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

//go:build linux || darwin
// +build linux darwin

package file

// Why not use go's mmap package?
// mmap doesn't support mapping in only part of a file.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
)

// mmapData holds both the page-aligned mapped region, and the actual requested data range
type mmapData struct {
	data         []byte
	originalData []byte
}

// mmap creates a memory-mapped region of the file
func Mmap(file *os.File, offset, length int64) (reader FileReaderAt, err error) {
	// Align offset to page boundary
	alignedOffset := offset & ^(mmapAlignment - 1)
	adjustment := offset - alignedOffset
	adjustedLength := length + adjustment

	// Map the region
	data, err := syscall.Mmap(int(file.Fd()), alignedOffset, int(adjustedLength), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Return the adjusted slice starting at the actual offset
	reader = &mmapData{
		data:         data[adjustment : adjustment+length],
		originalData: data,
	}
	runtime.SetFinalizer(reader, FileReaderAt.Close)
	return reader, err
}

func (m *mmapData) Close() error {
	if m.data == nil {
		return nil
	}
	m.data = nil
	originalData := m.originalData
	m.originalData = nil
	return syscall.Munmap(originalData)
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
