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

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
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

// writeItemOffsets writes (n+1) uint16 offStart for n |items|.
// the first offset is 0, the last offset is |sumSz|.
func writeItemOffsets(b *fb.Builder, items [][]byte, sumSz int) fb.UOffsetT {
	var off = sumSz
	for i := len(items) - 1; i >= 0; i-- {
		b.PrependUint16(uint16(off))
		off -= len(items[i])
	}
	assertTrue(off == 0, "incorrect final value after serializing offStart")
	b.PrependUint16(uint16(off))
	return b.EndVector(len(items) + 1)
}

func writeItemOffsets32(b *fb.Builder, items [][]byte, sumSz int) fb.UOffsetT {
	var off = sumSz
	for i := len(items) - 1; i >= 0; i-- {
		b.PrependUint32(uint32(off))
		off -= len(items[i])
	}
	assertTrue(off == 0, "incorrect final value after serializing offStart")
	b.PrependUint32(uint32(off))
	return b.EndVector(len(items) + 1)
}

// countAddresses returns the number of chunk addresses stored within |items|.
func countAddresses(items [][]byte, td val.TupleDesc) (cnt int) {
	for i := len(items) - 1; i >= 0; i-- {
		val.IterAddressFields(td, func(j int, t val.Type) {
			// get offset of address within |tup|
			addr := val.Tuple(items[i]).GetField(j)
			if len(addr) > 0 && !hash.New(addr).IsEmpty() {
				cnt++
			}
			return
		})
		val.IterAdaptiveFields(td, func(j int, t val.Type) {
			// get offset of adaptive encoded value within |tup|
			adaptiveValue := val.AdaptiveValue(val.Tuple(items[i]).GetField(j))
			if adaptiveValue.IsOutOfBand() {
				cnt++
			}
			return
		})
	}
	return
}

// writeAddressOffsets serializes an array of uint16 offStart representing address offStart within an array of items.
func writeAddressOffsets(b *fb.Builder, items [][]byte, sumSz int, td val.TupleDesc) fb.UOffsetT {
	var cnt int
	var off = sumSz
	for i := len(items) - 1; i >= 0; i-- {
		tup := val.Tuple(items[i])
		off -= len(tup) // start of tuple
		val.IterAddressFields(td, func(j int, t val.Type) {
			addr := tup.GetField(j)
			if len(addr) == 0 || hash.New(addr).IsEmpty() {
				return
			}
			// get offset of address within |tup|
			o, _ := tup.GetOffset(j)
			o += off // offset is tuple start plus field start
			b.PrependUint16(uint16(o))
			cnt++
		})
		val.IterAdaptiveFields(td, func(j int, t val.Type) {
			// get offset of adaptive encoded value within |tup|
			adaptiveValue := val.AdaptiveValue(val.Tuple(items[i]).GetField(j))
			if adaptiveValue.IsOutOfBand() {
				// Out-of-line adaptive values end in an address, so get the offset |hash.ByteLen| bytes before the end.
				o, _ := tup.GetOffset(j)
				o += off + len(adaptiveValue) - hash.ByteLen
				b.PrependUint16(uint16(o))
				cnt++
			}
			return
		})
	}
	return b.EndVector(cnt)
}

func writeCountArray(b *fb.Builder, counts []uint64) fb.UOffsetT {
	buf := make([]byte, maxEncodedSize(len(counts)))
	return b.CreateByteVector(encodeVarints(counts, buf))
}

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}
