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

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	cumulativeCountSize = val.ByteSize(6)
	nodeCountSize       = val.ByteSize(2)
	treeLevelSize       = val.ByteSize(1)

	maxNodeDataSize = uint64(math.MaxUint16)
)

// Node is a node in a prolly tree. Nodes are byte slices containing node items and
//   a footer. The footer contains offsets, an item count for the node, a cumulative
//   item count for the subtree rooted at this node, and this node's tree level.
//   Prolly trees are organized like a B+ Tree without linked leaf nodes. Internal
//   Nodes contain only keys and child pointers ("metaKeys" and "metaValues"). Leaf
//   Nodes contain keys and values. The offsets array enables random acces to items
//   with in a Node. The cumulative count field allows seeking into the tree by an
//   item's index number.
//
//   Node:
//     Items in a node are packed contiguously from the front of the byte slice.
//     For internal Nodes, metaKeys and metaValues are stored in alternating order
//     as separate items. MetaValues contain a chunk ref that can be resolved to a
//     child node using a NodeStore. MetaKeys store the largest key Tuple within
//     the subtree rooted at that child Node.
//   +--------+--------+-----+--------+--------+
//   | Item 0 | Item 1 | ... | Item N | Footer |
//   +--------+--------+--------------+--------+
//
//   Footer:
//   +---------------+------------------+------------+------------+
//   | Offsets Array | Cumulative Count | Node Count | Tree Level |
//   +---------------+------------------+------------+------------+
//
//   Offsets Array:
//     The offset array contains a uint16 for each node item after item 0. Offset i
//     encodes the byte distance from the front of the node to the beginning of the
//     ith item in the node. The offsets array for N items is 2*(N-1) bytes.
//   +----------+----------+-----+----------+
//   | Offset 1 | Offset 2 | ... | Offset N |
//   +----------+----------+-----+----------+
//
//   Cumulative Count:
//      The cumulative count is the total number of items in the subtree rooted at
//      this node. For leaf nodes, cumulative count is the same as node count.
//   +---------------------------+
//   | Cumulative Count (uint48) |
//   +---------------------------+
//
//   Node Count:
//      Node count is the number of items in this node.
//   +---------------------+
//   | Node Count (uint16) |
//   +---------------------+
//
//   Tree Level:
//      Tree Level is the height of this node within the tree. Leaf nodes are
//      level 0, the first level of internal nodes is level 1.
//   +--------------------+
//   | Tree Level (uint8) |
//   +--------------------+
//
//   Note: the current Node implementation is oriented toward implementing Map
//   semantics. However, Node could easily be modified to support Set semantics,
//   or other collections.
//
type Node []byte

type nodeItem []byte

func (i nodeItem) size() val.ByteSize {
	return val.ByteSize(len(i))
}

type nodePair [2]nodeItem

func (p nodePair) key() nodeItem {
	return p[0]
}

func (p nodePair) value() nodeItem {
	return p[1]
}

func makeProllyNode(pool pool.BuffPool, level uint64, items ...nodeItem) (node Node) {
	var sz uint64
	for _, item := range items {
		sz += uint64(item.size())

	}
	count := len(items)

	if sz > maxNodeDataSize {
		panic("items exceeded max chunk data size")
	}

	pos := val.ByteSize(sz)
	pos += val.OffsetsSize(count)
	pos += cumulativeCountSize
	pos += nodeCountSize
	pos += treeLevelSize

	node = pool.Get(uint64(pos))

	cc := countCumulativeItems(level, items)
	writeCumulativeCount(node, cc)
	writeItemCount(node, count)
	writeTreeLevel(node, level)

	pos = 0
	offs, _ := node.offsets()
	for i, item := range items {
		copy(node[pos:pos+item.size()], item)
		offs.Put(i, pos)
		pos += item.size()
	}

	return node
}

func countCumulativeItems(level uint64, items []nodeItem) (c uint64) {
	if level == 0 {
		return uint64(len(items))
	}

	for i := 1; i < len(items); i += 2 {
		c += metaValue(items[i]).GetCumulativeCount()
	}
	return c
}

func (nd Node) getItem(i int) nodeItem {
	offs, itemStop := nd.offsets()
	start, stop := offs.GetBounds(i, itemStop)
	return nodeItem(nd[start:stop])
}

func (nd Node) getPair(i int) (p nodePair) {
	offs, itemStop := nd.offsets()
	start, stop := offs.GetBounds(i, itemStop)
	p[0] = nodeItem(nd[start:stop])
	start, stop = offs.GetBounds(i+1, itemStop)
	p[1] = nodeItem(nd[start:stop])
	return
}

func (nd Node) size() val.ByteSize {
	return val.ByteSize(len(nd))
}

func (nd Node) level() int {
	sl := nd[nd.size()-treeLevelSize:]
	return int(val.ReadUint8(sl))
}

func (nd Node) nodeCount() int {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	return int(val.ReadUint16(nd[start:stop]))
}

func (nd Node) cumulativeCount() uint64 {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	buf := nd[start:stop]
	return val.ReadUint48(buf)
}

func (nd Node) offsets() (offs val.Offsets, itemStop val.ByteSize) {
	stop := nd.size() - treeLevelSize - nodeCountSize - cumulativeCountSize
	itemStop = stop - val.OffsetsSize(nd.nodeCount())
	return val.Offsets(nd[itemStop:stop]), itemStop
}

func (nd Node) leafNode() bool {
	return nd.level() == 0
}

func (nd Node) empty() bool {
	return len(nd) == 0 || nd.nodeCount() == 0
}

func writeTreeLevel(nd Node, level uint64) {
	nd[nd.size()-treeLevelSize] = uint8(level)
}

func writeItemCount(nd Node, count int) {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	val.WriteUint16(nd[start:stop], uint16(count))
}

func writeCumulativeCount(nd Node, count uint64) {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	val.WriteUint48(nd[start:stop], count)
}
