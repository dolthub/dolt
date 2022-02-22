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
	"github.com/dolthub/dolt/go/store/val"
)

const (
	maxVectorOffset = uint64(math.MaxUint16)
	refSize         = hash.ByteLen

	// These constants are mirrored from serial.TupleMap.KeyOffsetsLength()
	// and serial.TupleMap.ValueOffsetsLength() respectively.
	// They are only as stable as the flatbuffers schemas that define them.
	keyOffsetsVOffset   = 6
	valueOffsetsVOffset = 10
)

func init() {
	emptyNode = buildMapNode(sharedPool, 0, nil, nil)
}

var emptyNode Node

type Node struct {
	keys, values val.SlicedBuffer
	buf          serial.TupleMap
	count        uint16
}

func mapNodeFromBytes(bb []byte) Node {
	buf := serial.GetRootAsTupleMap(bb, 0)
	return mapNodeFromFlatbuffer(*buf)
}

func mapNodeFromFlatbuffer(buf serial.TupleMap) Node {
	keys := val.SlicedBuffer{
		Buf:  buf.KeyTuplesBytes(),
		Offs: getKeyOffsetsVector(buf),
	}
	values := val.SlicedBuffer{
		Buf:  buf.ValueTuplesBytes(),
		Offs: getValueOffsetsVector(buf),
	}

	count := buf.KeyOffsetsLength() + 1
	if len(keys.Buf) == 0 {
		count = 0
	}

	return Node{
		keys:   keys,
		values: values,
		count:  uint16(count),
		buf:    buf,
	}
}

func buildMapNode(pool pool.BuffPool, level uint64, keys, values []nodeItem) (node Node) {
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
	// todo(andy): tree count
	b.Finish(serial.TupleMapEnd(b))

	return mapNodeFromBytes(b.FinishedBytes())
}

func (nd Node) hashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) getKey(i int) nodeItem {
	return nd.keys.GetSlice(i)
}

func (nd Node) getValue(i int) nodeItem {
	if nd.leafNode() {
		return nd.values.GetSlice(i)
	} else {
		r := nd.getRef(i)
		return r[:]
	}
}

func (nd Node) getRef(i int) hash.Hash {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSize, (i+1)*refSize
	return hash.New(refs[start:stop])
}

// todo(andy): should we support this?
//func (nd Node) cumulativeCount() uint64 {
//	return nd.buf.TreeCount()
//}

func (nd Node) level() int {
	return int(nd.buf.TreeLevel())
}

func (nd Node) leafNode() bool {
	return nd.level() == 0
}

func (nd Node) empty() bool {
	return nd.bytes() == nil || nd.count == 0
}

func (nd Node) bytes() []byte {
	return nd.buf.Table().Bytes
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

func getKeyOffsetsVector(buf serial.TupleMap) []byte {
	sz := buf.KeyOffsetsLength() * 2
	tab := buf.Table()
	vec := tab.Offset(keyOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}

func getValueOffsetsVector(buf serial.TupleMap) []byte {
	sz := buf.ValueOffsetsLength() * 2
	tab := buf.Table()
	vec := tab.Offset(valueOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}
