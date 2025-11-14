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

package message

import (
	"unsafe"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

type offsetSize uint8

const (
	OFFSET_SIZE_16 offsetSize = iota
	OFFSET_SIZE_32
)

// ItemAccess accesses items in a serial.Message.
type ItemAccess struct {
	// bufStart is the offset to the start of the
	// Item buffer within a serial.Message.
	// bufLen is the length of the Item buffer.
	bufStart, bufLen uint32

	// offStart, if nonzero, is the offset to the
	// start of the uin16 offset buffer within a
	// serial.Message. A zero value for offStart
	// indicates an empty offset buffer.
	// bufLen is the length of the Item buffer.
	offStart, offLen uint32

	// If the serial.Message does not contain an
	// offset buffer (offStart is zero), then
	// Items have a fixed width equal to itemWidth.
	itemWidth  uint32
	offsetSize offsetSize
}

// GetItem returns the ith Item from the buffer.
func (acc *ItemAccess) GetItem(i int, msg serial.Message) []byte {
	bufStart := int(acc.bufStart)
	if acc.offStart != 0 {
		offStart := int(acc.offStart)
		var stop, start uint32
		switch acc.offsetSize {
		case OFFSET_SIZE_16:
			off := offStart + i*2
			b3 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+3)))
			b2 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+2)))
			b1 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+1)))
			b0 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off)))
			stop = uint32(b2) | (uint32(b3) << 8)
			start = uint32(b0) | (uint32(b1) << 8)
		case OFFSET_SIZE_32:
			off := offStart + i*4
			b7 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+7)))
			b6 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+6)))
			b5 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+5)))
			b4 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+4)))
			b3 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+3)))
			b2 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+2)))
			b1 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off+1)))
			b0 := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(msg))) + uintptr(off)))
			stop = uint32(b4) | (uint32(b5) << 8) | (uint32(b6) << 16) | (uint32(b7) << 24)
			start = uint32(b0) | (uint32(b1) << 8) | (uint32(b2) << 16) | (uint32(b3) << 24)
		}

		return msg[bufStart+int(start) : bufStart+int(stop)]
	} else {
		start := bufStart + int(acc.itemWidth)*i
		stop := start + int(acc.itemWidth)
		return msg[start:stop]
	}
}

func (acc *ItemAccess) IsEmpty() bool {
	return acc.bufLen == 0
}
