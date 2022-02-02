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
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	maxNodeDataSize = uint64(math.MaxUint16)
	refSz           = hash.ByteLen

	// todo(andy) tighter bound here
	fbPad = 96
)

func init() {
	emptyNode = makeMapNode(sharedPool, 0, nil, nil)
}

type mapNode struct {
	buf serial.TupleMap
	cnt int
}

var emptyNode mapNode

func makeMapNode(pool pool.BuffPool, level uint64, keys, values []nodeItem) (node mapNode) {
	var keySz, valSz int
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	b := getMapBuilder(pool, keySz+valSz+fbPad)

	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr           fb.UOffsetT
	)

	// serialize keys and offsets
	serial.TupleMapStartKeyTuplesVector(b, keySz)
	keyTups = b.EndVector(writeItems(b, keys))
	serial.TupleMapStartKeyOffsetsVector(b, len(keys)-1)
	keyOffs = b.EndVector(writeItemOffsets(b, keys, keySz))

	if level == 0 {
		// serialize ref tuples for leaf nodes
		serial.TupleMapStartKeyTuplesVector(b, valSz)
		valTups = b.EndVector(writeItems(b, values))
		serial.TupleMapStartValueOffsetsVector(b, len(values)-1)
		valOffs = b.EndVector(writeItemOffsets(b, values, valSz))
	} else {
		// serialize child refs for internal nodes
		serial.TupleMapStartRefArrayVector(b, valSz)
		refArr = b.EndVector(writeItems(b, values))
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
	// todo(andy): tree count
	b.Finish(serial.TupleMapEnd(b))

	return mapNodeFromBytes(b.FinishedBytes())
}

func getMapBuilder(pool pool.BuffPool, sz int) *fb.Builder {
	// todo(andy): initialize builder buffer from pool
	return fb.NewBuilder(sz)
}

func writeItems(b *fb.Builder, items []nodeItem) (cnt int) {
	for i := len(items) - 1; i >= 0; i-- {
		for j := len(items[i]) - 1; j >= 0; j-- {
			b.PrependByte(items[i][j])
			cnt++
		}
	}
	return
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

func mapNodeFromBytes(bb []byte) mapNode {
	buf := serial.GetRootAsTupleMap(bb, 0)
	// first key offset omitted
	cnt := buf.KeyOffsetsLength() + 1
	if len(buf.KeyTuplesBytes()) == 0 {
		cnt = 0
	}
	return mapNode{
		buf: *buf,
		cnt: cnt,
	}
}

func (nd mapNode) hashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd mapNode) getKey(i int) nodeItem {
	keys := nd.buf.KeyTuplesBytes()

	start, stop := uint16(0), uint16(len(keys))
	if i > 0 {
		start = nd.buf.KeyOffsets(i - 1)
	}
	if i < nd.buf.KeyOffsetsLength() {
		stop = nd.buf.KeyOffsets(i)
	}

	if start == stop {
		panic("fux")
	}

	return keys[start:stop]
}

func (nd mapNode) getValue(i int) nodeItem {
	if nd.leafNode() {
		return nd.getValueTuple(i)
	} else {
		r := nd.getRef(i)
		return r[:]
	}
}

func (nd mapNode) getValueTuple(i int) nodeItem {
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

func (nd mapNode) getRef(i int) hash.Hash {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSz, (i+1)*refSz
	return hash.New(refs[start:stop])
}

func (nd mapNode) level() int {
	return int(nd.buf.TreeLevel())
}

func (nd mapNode) nodeCount() int {
	return nd.cnt
}

func (nd mapNode) cumulativeCount() uint64 {
	return nd.buf.TreeCount()
}

func (nd mapNode) leafNode() bool {
	return nd.level() == 0
}

func (nd mapNode) empty() bool {
	return nd.bytes() == nil || nd.nodeCount() == 0
}

func (nd mapNode) bytes() []byte {
	return nd.buf.Table().Bytes
}
