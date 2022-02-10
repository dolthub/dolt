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

type SlicedBuffer struct {
	Buf  []byte
	Offs offsets
}

func slicedTupleBuffer(tup Tuple) SlicedBuffer {
	mask := tup.mask()
	offStop := tup.size() - numFieldsSize - mask.size()
	bufStop := offStop - offsetsSize(mask.count())

	return SlicedBuffer{
		Buf:  tup[:bufStop],
		Offs: offsets(tup[bufStop:offStop]),
	}
}

// GetSlice returns the ith slice of |sb.Buf|.
func (sb SlicedBuffer) GetSlice(i int) []byte {
	start := sb.Offs.getOffset(i)
	stop := ByteSize(len(sb.Buf))
	if !sb.isLastIndex(i) {
		stop = sb.Offs.getOffset(i + 1)
	}
	return sb.Buf[start:stop]
}

// isLastIndex returns true if |i| is the last index in |sl|.
func (sb SlicedBuffer) isLastIndex(i int) bool {
	return len(sb.Offs) == i*2
}

type offsets []byte

// offsetsSize returns the number of bytes needed to
// store |fieldCount| offsets.
func offsetsSize(count int) ByteSize {
	if count == 0 {
		return 0
	}
	return ByteSize((count - 1) * 2)
}

// getOffset gets the byte position of the _start_ of element |i|.
func (os offsets) getOffset(i int) ByteSize {
	if i == 0 {
		return 0
	}
	start := (i - 1) * 2
	off := readUint16(os[start : start+2])
	return ByteSize(off)
}

// putOffset writes offset |pos| at index |i|.
func (os offsets) putOffset(i int, off ByteSize) {
	if i == 0 {
		return
	}
	start := (i - 1) * 2
	writeUint16(os[start:start+2], uint16(off))
}
