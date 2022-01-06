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

package val

// todo(andy): more ergonomic offsets
// type SlicedBuffer struct {
//    buf  []byte
//    offs []uint16
// }

type Offsets []byte

// OffsetsSize returns the number of bytes needed to
// store |fieldCount| offsets.
func OffsetsSize(count int) ByteSize {
	if count == 0 {
		return 0
	}
	return ByteSize((count - 1) * 2)
}

// Count returns the number of offsets stored in |sl|.
func (os Offsets) Count() int {
	return (len(os) / 2) + 1
}

// GetBounds returns the ith offset. |last| is the byte position
// of the _end_ of the last element.
func (os Offsets) GetBounds(i int, last ByteSize) (start, stop ByteSize) {
	start = os.getOffset(i)
	if os.isLastIndex(i) {
		stop = last
	} else {
		stop = os.getOffset(i + 1)
	}
	return
}

// getOffset gets the byte position of the _start_ of element |i|.
func (os Offsets) getOffset(i int) ByteSize {
	if i == 0 {
		return 0
	}
	start := (i - 1) * 2
	off := ReadUint16(os[start : start+2])
	return ByteSize(off)
}

// Put writes offset |pos| at index |i|.
func (os Offsets) Put(i int, off ByteSize) {
	if i == 0 {
		return
	}
	start := (i - 1) * 2
	WriteUint16(os[start:start+2], uint16(off))
}

// isLastIndex returns true if |i| is the last index in |sl|.
func (os Offsets) isLastIndex(i int) bool {
	return len(os) == i*2
}
