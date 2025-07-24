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

//go:build windows
// +build windows

package file

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// mmapData holds both the page-aligned mapped region, and the actual requested data range
type mmapData struct {
	data    []byte
	mapView uintptr // a pointer the start of the page-aligned mapped region
}

func (r *mmapData) Close() error {
	if r.data == nil {
		return nil
	} else if len(r.data) == 0 {
		r.data = nil
		return nil
	}
	r.data = nil
	runtime.SetFinalizer(r, nil)
	return syscall.UnmapViewOfFile(r.mapView)
}

// Open memory-maps the named file for reading.
func Mmap(file *os.File, offset, length int64) (reader FileReaderAt, err error) {
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fi.Size()
	if fileSize == 0 || length == 0 {
		// If the requested range is 0 bytes, we don't need to make the syscall, or set the finalizer.
		return &mmapData{}, nil
	}

	// Align offset to allocation granularity (64KB on Windows)
	alignedOffset := offset & ^(mmapAlignment - 1)
	adjustment := offset - alignedOffset
	adjustedLength := length + adjustment

	fileSizeLow, fileSizeHigh := uint32(fileSize), uint32(fileSize>>32)
	fmap, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, fileSizeHigh, fileSizeLow, nil)
	if err != nil {
		return nil, err
	}

	defer syscall.CloseHandle(fmap)
	offsetLow, offsetHigh := uint32(alignedOffset), uint32(alignedOffset>>32)
	mapView, err := syscall.MapViewOfFile(fmap, syscall.FILE_MAP_READ, offsetHigh, offsetLow, uintptr(adjustedLength))
	if err != nil {
		return nil, err
	}

	data := (*[1 << 30]byte)(unsafe.Pointer(mapView))[adjustment : adjustment+length]

	reader = &mmapData{
		data:    data,
		mapView: mapView,
	}
	runtime.SetFinalizer(reader, FileReaderAt.Close)

	return reader, nil
}
