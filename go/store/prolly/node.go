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
	"encoding/binary"
	"math"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	cumulativeCountSize = val.ByteSize(6)
	nodeCountSize       = val.ByteSize(2)
	treeLevelSize       = val.ByteSize(1)

	maxNodeDataSize = val.ByteSize(math.MaxUint16)
)

type nodeItem []byte

func (i nodeItem) size() val.ByteSize {
	return val.ByteSize(len(i))
}

type node []byte

func makeProllyNode(pool pool.BuffPool, level uint64, items ...nodeItem) (nd node) {
	var cumulativeCount uint64
	var pos val.ByteSize
	for _, item := range items {
		pos += item.size()
		cumulativeCount += cumulativeCountFromItem(level, item)
	}
	count := len(items)

	pos += val.OffsetsSize(count)
	pos += cumulativeCountSize
	pos += nodeCountSize
	pos += treeLevelSize

	nd = pool.Get(uint64(pos))

	writeCumulativeCount(nd, cumulativeCount)
	writeItemCount(nd, count)
	writeTreeLevel(nd, level)

	pos = 0
	offs, _ := nd.offsets()
	for i, item := range items {
		copy(nd[pos:pos+item.size()], item)
		offs.Put(i, pos)
		pos += item.size()
	}

	return nd
}

func cumulativeCountFromItem(level uint64, item nodeItem) uint64 {
	if level == 0 {
		return 1
	}
	return metaTuple(item).GetCumulativeCount()
}

func (nd node) getItem(i int) nodeItem {
	offs, itemStop := nd.offsets()

	start := offs.Get(i)

	var stop val.ByteSize
	if offs.IsLastIndex(i) {
		stop = itemStop
	} else {
		stop = offs.Get(i + 1)
	}

	return nodeItem(nd[start:stop])
}

func (nd node) size() val.ByteSize {
	return val.ByteSize(len(nd))
}

func (nd node) level() int {
	return int(nd[nd.size()-treeLevelSize])
}

func (nd node) nodeCount() int {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	return int(binary.LittleEndian.Uint16(nd[start:stop]))
}

func (nd node) cumulativeCount() uint64 {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	buf := nd[start:stop]
	return readUint48(buf)
}

func (nd node) offsets() (offs val.Offsets, itemStop val.ByteSize) {
	stop := nd.size() - treeLevelSize - nodeCountSize - cumulativeCountSize
	itemStop = stop - val.OffsetsSize(nd.nodeCount())
	return val.Offsets(nd[itemStop:stop]), itemStop
}

func (nd node) leafNode() bool {
	return nd.level() == 0
}

func (nd node) empty() bool {
	return len(nd) == 0 || nd.nodeCount() == 0
}

func writeTreeLevel(nd node, level uint64) {
	nd[nd.size()-treeLevelSize] = uint8(level)
}

func writeItemCount(nd node, count int) {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	binary.LittleEndian.PutUint16(nd[start:stop], uint16(count))
}

func writeCumulativeCount(nd node, count uint64) {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	writeUint48(nd[start:stop], count)
}

const (
	uint48Size = 6
	uint48Max  = uint64(1<<48 - 1)
)

func writeUint48(dest []byte, u uint64) {
	if len(dest) != uint48Size {
		panic("incorrect number of bytes for uint48")
	}
	if u > uint48Max {
		panic("uint is greater than max uint")
	}

	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], u)
	copy(dest, tmp[:uint48Size])
}

func readUint48(src []byte) (u uint64) {
	if len(src) != uint48Size {
		panic("incorrect number of bytes for uint48")
	}
	var tmp [8]byte
	copy(tmp[:uint48Size], src)
	u = binary.LittleEndian.Uint64(tmp[:])
	return
}
