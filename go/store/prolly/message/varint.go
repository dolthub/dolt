// Copyright 2022 Dolthub, Inc.
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
	"encoding/binary"
)

func encodeVarints(ints []uint64, buf []byte) []byte {
	return endcodeSignedDeltas(ints, buf)
}

func decodeVarints(buf []byte, ints []uint64) []uint64 {
	return decodeSignedDeltas(buf, ints)
}

func maxEncodedSize(n int) int {
	return (n + 1) * binary.MaxVarintLen64
}

func endcodeSignedDeltas(ints []uint64, buf []byte) []byte {
	pos, prev := 0, int64(0)
	for i := range ints {
		curr := int64(ints[i])
		delta := curr - prev
		prev = curr

		n := binary.PutVarint(buf[pos:], delta)
		pos += n
	}
	return buf[:pos]
}

func decodeSignedDeltas(buf []byte, ints []uint64) []uint64 {
	prev := int64(0)
	for i := range ints {
		delta, n := binary.Varint(buf)
		buf = buf[n:]

		curr := prev + delta
		ints[i] = uint64(curr)
		prev = curr
	}
	assertTrue(len(buf) == 0)
	return ints
}
