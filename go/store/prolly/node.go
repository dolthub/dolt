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

package prolly

import (
	"fmt"
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	maxVectorOffset = uint64(math.MaxUint16)
	refSize         = hash.ByteLen
)

func init() {
	//emptyNode = makeMapNode(sharedPool, 0, nil, nil)
}

type Node struct {
	buf serial.TupleMap
	cnt int
}

var emptyNode Node

func makeMapNode(pool pool.BuffPool, level uint64, keys, values []nodeItem) (node Node) {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr           fb.UOffsetT
	)

	keySz, valSz, bufSz := measureNodeSize(keys, values)
	b := getMapBuilder(pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, keys, keySz)
	serial.TupleMapStartKeyOffsetsVector(b, len(keys)-1)
	keyOffs = b.EndVector(writeItemOffsets(b, keys, keySz))

	if level == 0 {
		// serialize ref tuples for leaf nodes
		valTups = writeItemBytes(b, values, valSz)
		serial.TupleMapStartValueOffsetsVector(b, len(values)-1)
		valOffs = b.EndVector(writeItemOffsets(b, values, valSz))
	} else {
		// serialize child refs for internal nodes
		refArr = writeItemBytes(b, values, valSz)
	}

	// populate the node's vtable
	serial.TupleMapStart(b)
	serial.TupleMapAddKeyTuples(b, keyTups)
	serial.TupleMapAddKeyOffsets(b, keyOffs)
	if level == 0 {
		serial.TupleMapAddValueTuples(b, valTups)
		serial.TupleMapAddValueOffsets(b, valOffs)
	} else {
		serial.TupleMapAddRefArray(b, refArr)
	}
	serial.TupleMapAddKeyFormat(b, serial.TupleFormatV1)
	serial.TupleMapAddValueFormat(b, serial.TupleFormatV1)
	serial.TupleMapAddTreeLevel(b, byte(level))
	// todo(andy): tree empty
	b.Finish(serial.TupleMapEnd(b))

	return mapNodeFromBytes(b.FinishedBytes())
}

func getMapBuilder(pool pool.BuffPool, sz int) *fb.Builder {
	// todo(andy): initialize builder buffer from pool
	return fb.NewBuilder(sz)
}

// measureNodeSize returns the exact size of the tuple vectors for keys and values,
// and an estimate of the overall size of the final flatbuffer.
func measureNodeSize(keys, values []nodeItem) (keySz, valSz, bufSz int) {
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}

	// constraints enforced upstream
	if keySz > int(maxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds size limit ( %d > %d )", keySz, maxVectorOffset))
	}
	if valSz > int(maxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds size limit ( %d > %d )", valSz, maxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += len(keys)*2 + len(values)*2 // offsets
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)

	return
}

func writeItemBytes(b *fb.Builder, items []nodeItem, sumSz int) fb.UOffsetT {
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

func writeItemOffsets(b *fb.Builder, items []nodeItem, sz int) (cnt int) {
	off := sz
	for i := len(items) - 1; i > 0; i-- { // omit first offset
		off -= len(items[i])
		b.PrependUint16(uint16(off))
		cnt++
	}
	return
}

func mapNodeFromBytes(bb []byte) Node {
	buf := serial.GetRootAsTupleMap(bb, 0)
	// first key offset omitted
	cnt := buf.KeyOffsetsLength() + 1
	if len(buf.KeyTuplesBytes()) == 0 {
		cnt = 0
	}
	return Node{
		buf: *buf,
		cnt: cnt,
	}
}

func (nd Node) hashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) getKey(i int) nodeItem {
	keys := nd.buf.KeyTuplesBytes()

	start, stop := uint16(0), uint16(len(keys))
	if i > 0 {
		start = nd.buf.KeyOffsets(i - 1)
	}
	if i < nd.buf.KeyOffsetsLength() {
		stop = nd.buf.KeyOffsets(i)
	}

	return keys[start:stop]
}

func (nd Node) getValue(i int) nodeItem {
	if nd.leafNode() {
		return nd.getValueTuple(i)
	} else {
		r := nd.getRef(i)
		return r[:]
	}
}

func (nd Node) getValueTuple(i int) nodeItem {
	values := nd.buf.ValueTuplesBytes()

	start, stop := uint16(0), uint16(len(values))
	if i > 0 {
		start = nd.buf.ValueOffsets(i - 1)
	}
	if i < nd.buf.ValueOffsetsLength() {
		stop = nd.buf.ValueOffsets(i)
	}

	return values[start:stop]
}

func (nd Node) getRef(i int) hash.Hash {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSize, (i+1)*refSize
	return hash.New(refs[start:stop])
}

func (nd Node) level() int {
	return int(nd.buf.TreeLevel())
}

func (nd Node) nodeCount() int {
	return nd.cnt
}

// todo(andy): should we support this?
//func (nd Node) cumulativeCount() uint64 {
//	return nd.buf.TreeCount()
//}

func (nd Node) leafNode() bool {
	return nd.level() == 0
}

func (nd Node) empty() bool {
	return nd.bytes() == nil || nd.nodeCount() == 0
}

func (nd Node) bytes() []byte {
	return nd.buf.Table().Bytes
}
