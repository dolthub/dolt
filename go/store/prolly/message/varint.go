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

import "encoding/binary"

func maxEncodedSize(n int) int {
	return n * binary.MaxVarintLen64
}

func encodeVarints(ints []uint64, buf []byte) []byte {
	pos := 0
	for _, count := range ints {
		n := binary.PutUvarint(buf[pos:], count)
		pos += n
	}
	return buf[:pos]
}

func decodeVarints(buf []byte, ints []uint64) []uint64 {
	for i := range ints {
		var k int
		ints[i], k = binary.Uvarint(buf)
		buf = buf[k:]
	}
	assertTrue(len(buf) == 0)
	return ints
}
