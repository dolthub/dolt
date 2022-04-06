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
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	nodeBuilderListSize = 256
)

type novelNode struct {
	node      Node
	addr      hash.Hash
	lastKey   nodeItem
	treeCount uint64
}

func writeNewNode(ctx context.Context, ns NodeStore, bld *nodeBuilder) (novelNode, error) {
	node := bld.build(ns.Pool())

	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}

	fmt.Println(formatHack(addr, bld))

	var lastKey val.Tuple
	if len(bld.keys) > 0 {
		lastKey = val.Tuple(bld.keys[len(bld.keys)-1])
		lastKey = val.CloneTuple(ns.Pool(), lastKey)
	}

	treeCount := uint64(node.treeCount())

	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   nodeItem(lastKey),
		treeCount: treeCount,
	}, nil
}

type nodeBuilder struct {
	keys, values []nodeItem
	size, level  int

	subtrees subtreeCounts
}

func (nb *nodeBuilder) hasCapacity(key, value nodeItem) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(maxVectorOffset)
}

func (nb *nodeBuilder) appendItems(key, value nodeItem, subtree uint64) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *nodeBuilder) nodeCount() int {
	return len(nb.keys)
}

func (nb *nodeBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	nb.keys = nb.keys[:0]
	nb.values = nb.values[:0]
	nb.size = 0
	nb.subtrees = nb.subtrees[:0]
}

func (nb *nodeBuilder) firstChildRef() hash.Hash {
	return hash.New(nb.values[0])
}

func (nb *nodeBuilder) build(pool pool.BuffPool) (node Node) {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := measureNodeSize(nb.keys, nb.values, nb.subtrees)
	b := getMapBuilder(pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, nb.keys, keySz)
	serial.TupleMapStartKeyOffsetsVector(b, len(nb.keys)-1)
	keyOffs = b.EndVector(writeItemOffsets(b, nb.keys, keySz))

	if nb.level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, nb.values, valSz)
		serial.TupleMapStartValueOffsetsVector(b, len(nb.values)-1)
		valOffs = b.EndVector(writeItemOffsets(b, nb.values, valSz))
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, nb.values, valSz)
		cardArr = writeCountArray(b, nb.subtrees)
	}

	// populate the node's vtable
	serial.TupleMapStart(b)
	serial.TupleMapAddKeyTuples(b, keyTups)
	serial.TupleMapAddKeyOffsets(b, keyOffs)
	if nb.level == 0 {
		serial.TupleMapAddValueTuples(b, valTups)
		serial.TupleMapAddValueOffsets(b, valOffs)
		serial.TupleMapAddTreeCount(b, uint64(len(nb.keys)))
	} else {
		serial.TupleMapAddRefArray(b, refArr)
		serial.TupleMapAddRefCardinalities(b, cardArr)
		serial.TupleMapAddTreeCount(b, nb.subtrees.sum())
	}
	serial.TupleMapAddKeyFormat(b, serial.TupleFormatV1)
	serial.TupleMapAddValueFormat(b, serial.TupleFormatV1)
	serial.TupleMapAddTreeLevel(b, uint8(nb.level))
	b.Finish(serial.TupleMapEnd(b))

	buf := b.FinishedBytes()
	return mapNodeFromBytes(buf)
}

func newSubtreeCounts(count int) subtreeCounts {
	return make([]uint64, 0, count)
}

type subtreeCounts []uint64

func (sc subtreeCounts) sum() (s uint64) {
	for _, count := range sc {
		s += count
	}
	return
}

func readSubtreeCounts(n int, buf []byte) (sc subtreeCounts) {
	sc = make([]uint64, 0, n)
	for len(buf) > 0 {
		count, n := binary.Uvarint(buf)
		sc = append(sc, count)
		buf = buf[n:]
	}
	return
}

func writeSubtreeCounts(sc subtreeCounts) []byte {
	buf := make([]byte, len(sc)*binary.MaxVarintLen64)
	pos := 0
	for _, count := range sc {
		n := binary.PutUvarint(buf[pos:], count)
		pos += n
	}
	return buf[:pos]
}

func newNodeBuilder(level int) *nodeBuilder {
	return &nodeBuilder{
		keys:     make([]nodeItem, 0, nodeBuilderListSize),
		values:   make([]nodeItem, 0, nodeBuilderListSize),
		subtrees: newSubtreeCounts(nodeBuilderListSize),
		level:    level,
	}
}

func getMapBuilder(pool pool.BuffPool, sz int) (b *fb.Builder) {
	b = fb.NewBuilder(0)
	buf := pool.Get(uint64(sz))
	b.Bytes = buf[:0]
	return
}

// measureNodeSize returns the exact size of the tuple vectors for keys and values,
// and an estimate of the overall size of the final flatbuffer.
func measureNodeSize(keys, values []nodeItem, subtrees []uint64) (keySz, valSz, bufSz int) {
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	refCntSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > int(maxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds size limit ( %d > %d )", keySz, maxVectorOffset))
	}
	if valSz > int(maxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds size limit ( %d > %d )", valSz, maxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
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

func writeCountArray(b *fb.Builder, sc subtreeCounts) fb.UOffsetT {
	// todo(andy) write without copy
	arr := writeSubtreeCounts(sc)
	return b.CreateByteVector(arr)
}

func formatHack(addr hash.Hash, bld *nodeBuilder) string {
	desc := val.NewTupleDescriptor(val.Type{Enc: val.Int32Enc})
	return formatCompletedNode(addr, bld, desc, desc)
}

func formatCompletedNode(addr hash.Hash, bld *nodeBuilder, kd, vd val.TupleDesc) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Novel Node (level %d) #%s { \n", bld.level, addr.String()))
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
