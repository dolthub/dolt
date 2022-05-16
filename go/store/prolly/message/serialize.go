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
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	MaxVectorOffset = uint64(math.MaxUint16)
)

func getFlatbufferBuilder(pool pool.BuffPool, sz int) (b *fb.Builder) {
	b = fb.NewBuilder(0)
	b.Bytes = pool.Get(uint64(sz))
	b.Reset()
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

func writeCountArray(b *fb.Builder, counts []uint64) fb.UOffsetT {
	// todo(andy): encode without alloc
	buf := make([]byte, maxEncodedSize(len(counts)))
	return b.CreateByteVector(encodeVarints(counts, buf))
}

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}
