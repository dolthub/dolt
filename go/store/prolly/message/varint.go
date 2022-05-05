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
	"math"
)

func encodeVarints(ints []uint64, buf []byte) []byte {
	return encodeMinDeltas(ints, buf)
}

func decodeVarints(buf []byte, ints []uint64) []uint64 {
	return decodeMinDeltas(buf, ints)
}

func maxEncodedSize(n int) int {
	return (n + 1) * binary.MaxVarintLen64
}

// encodeMinDeltas encodes an unsorted array |ints|.
// The encoding format attempts to minimize encoded size by
// first finding and encoding the minimum value of |ints|
// and then encoding the difference between each value and
// that minimum.
func encodeMinDeltas(ints []uint64, buf []byte) []byte {
	min := uint64(math.MaxUint64)
	for i := range ints {
		if min > ints[i] {
			min = ints[i]
		}
	}

	pos := 0
	pos += binary.PutUvarint(buf[pos:], min)

	for _, count := range ints {
		delta := count - min
		pos += binary.PutUvarint(buf[pos:], delta)
	}
	return buf[:pos]
}

// decodeMinDeltas decodes an array of ints that were
// previously encoded with encodeMinDeltas.
func decodeMinDeltas(buf []byte, ints []uint64) []uint64 {
	min, k := binary.Uvarint(buf)
	buf = buf[k:]
	for i := range ints {
		delta, k := binary.Uvarint(buf)
		buf = buf[k:]
		ints[i] = min + delta
	}
	assertTrue(len(buf) == 0)
	return ints
}
