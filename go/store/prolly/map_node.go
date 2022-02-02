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

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	refSz = hash.ByteLen
)

const (
	cumulativeCountSize = val.ByteSize(6)
	nodeCountSize       = val.ByteSize(2)
	treeLevelSize       = val.ByteSize(1)

	maxNodeDataSize = uint64(math.MaxUint16)
)

var emptyNode = mapNode{}

type mapNode struct {
	buf serial.TupleMap

	// todo(andy): embed hash.Hash here?
}

func makeProllyNode(pool pool.BuffPool, level uint64, items ...nodeItem) (node mapNode) {
	//var sz uint64
	//for _, item := range items {
	//	sz += uint64(item.size())
	//
	//}
	//count := len(items)
	//
	//if sz > maxNodeDataSize {
	//	panic("items exceeded max chunk data size")
	//}
	//
	//pos := val.ByteSize(sz)
	//pos += val.OffsetsSize(count)
	//pos += cumulativeCountSize
	//pos += nodeCountSize
	//pos += treeLevelSize
	//
	//node = pool.Get(uint64(pos))
	//
	//cc := countCumulativeItems(level, items)
	//writeCumulativeCount(node, cc)
	//writeItemCount(node, count)
	//writeTreeLevel(node, level)
	//
	//pos = 0
	//offs, _ := node.offsets()
	//for i, item := range items {
	//	copy(node[pos:pos+item.size()], item)
	//	offs.Put(i, pos)
	//	pos += item.size()
	//}
	//
	//return node

	return mapNode{}
}

func mapNodeFromBytes(bb []byte) mapNode {
	fb := serial.GetRootAsTupleMap(bb, 0)
	return mapNode{buf: *fb}
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

	return keys[start:stop]
}

func (nd mapNode) getValue(i int) nodeItem {
	if nd.leafNode() {
		return nd.getValueTuple(i)
	} else {
		return nd.getRef(i)
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

func (nd mapNode) getRef(i int) nodeItem {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSz, (i+1)*refSz
	return refs[start:stop]
}

func (nd mapNode) level() int {
	return int(nd.buf.TreeLevel())
}

func (nd mapNode) nodeCount() int {
	// first offset omitted
	return nd.buf.KeyOffsetsLength() + 1
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
