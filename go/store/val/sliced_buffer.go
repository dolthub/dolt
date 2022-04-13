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

// GetSlice returns the ith slice of |sb.Buf|.
func (sb SlicedBuffer) GetSlice(i int) []byte {
	start := uint16(0)
	if i > 0 {
		pos := (i - 1) * 2
		start = readUint16(sb.Offs[pos : pos+2])
	}

	stop := uint16(len(sb.Buf))
	if i*2 < len(sb.Offs) {
		pos := i * 2
		stop = readUint16(sb.Offs[pos : pos+2])
	}

	return sb.Buf[start:stop]
}

func (sb SlicedBuffer) Len() int {
	// offsets stored as uint16s with first offset omitted
	return len(sb.Offs)/2 + 1
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

// writeOffset writes offset |pos| at index |i|.
func writeOffset(i int, off ByteSize, arr offsets) {
	if i == 0 {
		return
	}
	start := (i - 1) * 2
	writeUint16(arr[start:start+2], uint16(off))
}
