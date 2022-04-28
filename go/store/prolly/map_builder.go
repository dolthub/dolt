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

package prolly

import (
	"encoding/binary"
	"fmt"
	"strings"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func newEmptyNode(pool pool.BuffPool) tree.Node {
	bld := &nodeBuilder{}
	return bld.Build(pool)
}

func newNodeBuilder(level int) *nodeBuilder {
	return &nodeBuilder{level: level}
}

var _ tree.NodeBuilderFactory[*nodeBuilder] = newNodeBuilder

type nodeBuilder struct {
	keys, values []tree.Item
	size, level  int

	subtrees tree.SubtreeCounts
}

var _ tree.NodeBuilder = &nodeBuilder{}

func (nb *nodeBuilder) StartNode() {
	nb.reset()
}

func (nb *nodeBuilder) HasCapacity(key, value tree.Item) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(tree.MaxVectorOffset)
}

func (nb *nodeBuilder) AddItems(key, value tree.Item, subtree uint64) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *nodeBuilder) Count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder) Build(pool pool.BuffPool) (node tree.Node) {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := measureNodeSize(nb.keys, nb.values, nb.subtrees)
	b := getMapBuilder(pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, nb.keys, keySz)
	serial.ProllyTreeNodeStartKeyOffsetsVector(b, len(nb.keys)-1)
	keyOffs = b.EndVector(writeItemOffsets(b, nb.keys, keySz))

	if nb.level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, nb.values, valSz)
		serial.ProllyTreeNodeStartValueOffsetsVector(b, len(nb.values)-1)
		valOffs = b.EndVector(writeItemOffsets(b, nb.values, valSz))
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, nb.values, valSz)
		cardArr = writeCountArray(b, nb.subtrees)
	}

	// populate the node's vtable
	serial.ProllyTreeNodeStart(b)
	serial.ProllyTreeNodeAddKeyItems(b, keyTups)
	serial.ProllyTreeNodeAddKeyOffsets(b, keyOffs)
	if nb.level == 0 {
		serial.ProllyTreeNodeAddValueItems(b, valTups)
		serial.ProllyTreeNodeAddValueOffsets(b, valOffs)
		serial.ProllyTreeNodeAddTreeCount(b, uint64(len(nb.keys)))
	} else {
		serial.ProllyTreeNodeAddAddressArray(b, refArr)
		serial.ProllyTreeNodeAddSubtreeCounts(b, cardArr)
		serial.ProllyTreeNodeAddTreeCount(b, nb.subtrees.Sum())
	}
	serial.ProllyTreeNodeAddKeyType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddValueType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddTreeLevel(b, uint8(nb.level))
	b.Finish(serial.ProllyTreeNodeEnd(b))
	nb.reset()

	buf := b.FinishedBytes()
	return tree.NodeFromBytes(buf)
}

func (nb *nodeBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	nb.keys = nb.keys[:0]
	nb.values = nb.values[:0]
	nb.size = 0
	nb.subtrees = nb.subtrees[:0]
}

func getMapBuilder(pool pool.BuffPool, sz int) (b *fb.Builder) {
	b = fb.NewBuilder(0)
	buf := pool.Get(uint64(sz))
	b.Bytes = buf[:0]
	return
}

// measureNodeSize returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func measureNodeSize(keys, values []tree.Item, subtrees []uint64) (keySz, valSz, bufSz int) {
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	refCntSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > int(tree.MaxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds Size limit ( %d > %d )", keySz, tree.MaxVectorOffset))
	}
	if valSz > int(tree.MaxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds Size limit ( %d > %d )", valSz, tree.MaxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offsets
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)

	return
}

func writeItemBytes(b *fb.Builder, items []tree.Item, sumSz int) fb.UOffsetT {
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

func writeItemOffsets(b *fb.Builder, items []tree.Item, sz int) (cnt int) {
	off := sz
	for i := len(items) - 1; i > 0; i-- { // omit first offset
		off -= len(items[i])
		b.PrependUint16(uint16(off))
		cnt++
	}
	return
}

func writeCountArray(b *fb.Builder, sc tree.SubtreeCounts) fb.UOffsetT {
	// todo(andy) write without copy
	arr := tree.WriteSubtreeCounts(sc)
	return b.CreateByteVector(arr)
}

func formatCompletedNode(addr hash.Hash, bld *nodeBuilder, kd, vd val.TupleDesc) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Novel Node (Level %d) #%s { \n", bld.level, addr.String()))
	for i := range bld.keys {
		k, v := bld.keys[i], bld.values[i]
		sb.WriteString("\t")
		sb.WriteString(kd.Format(val.Tuple(k)))
		sb.WriteString(": ")
		if bld.level == 0 {
			sb.WriteString(vd.Format(val.Tuple(v)))
		} else {
			sb.WriteString("#")
			sb.WriteString(hash.New(v).String())
		}
		sb.WriteString(",\n")
	}
	sb.WriteString("} ")
	return sb.String()
}
