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

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	MaxVectorOffset = uint64(math.MaxUint16)
)

func getFlatbufferBuilder(pool pool.BuffPool, sz int) (b *fb.Builder) {
	b = fb.NewBuilder(0)
	buf := pool.Get(uint64(sz))
	b.Bytes = buf[:0]
	return
}

func writeItemBytes(b *fb.Builder, items [][]byte, sumSz int) fb.UOffsetT {
	b.Prep(fb.SizeUOffsetT, sumSz)

	stop := int(b.Head())
	start := stop - sumSz
	for _, item := range items {
		copy(b.Bytes[start:stop], item)
		start += len(item)
	}

	start = stop - sumSz
	return b.CreateByteVector(b.Bytes[start:stop])
}

func writeItemOffsets(b *fb.Builder, items [][]byte, sumSz int) fb.UOffsetT {
	var cnt int
	var off = sumSz
	for i := len(items) - 1; i > 0; i-- { // omit first offset
		off -= len(items[i])
		b.PrependUint16(uint16(off))
		cnt++
	}
	return b.EndVector(cnt)
}

func writeCountArray(b *fb.Builder, sc []uint64) fb.UOffsetT {
	// todo(andy) write without copy
	arr := WriteSubtreeCounts(sc)
	return b.CreateByteVector(arr)
}

func readSubtreeCounts(n int, buf []byte) (sc []uint64) {
	sc = make([]uint64, 0, n)
	for len(buf) > 0 {
		count, n := binary.Uvarint(buf)
		sc = append(sc, count)
		buf = buf[n:]
	}
	assertTrue(len(sc) == n)
	return
}

func WriteSubtreeCounts(sc []uint64) []byte {
	buf := make([]byte, len(sc)*binary.MaxVarintLen64)
	pos := 0
	for _, count := range sc {
		n := binary.PutUvarint(buf[pos:], count)
		pos += n
	}
	return buf[:pos]
}

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}
