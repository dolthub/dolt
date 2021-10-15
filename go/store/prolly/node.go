// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"encoding/binary"
	"math"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	// todo(andy): include cumulative size
	treeLevelSize = val.ByteSize(1)
	itemCountSize = val.ByteSize(2)

	maxNodeDataSize = val.ByteSize(math.MaxUint16)
)

type nodeItem []byte

func (i nodeItem) size() val.ByteSize {
	return val.ByteSize(len(i))
}

type node []byte

func makeProllyNode(pool pool.BuffPool, level uint64, items ...nodeItem) (nd node) {
	var pos val.ByteSize
	for _, item := range items {
		pos += item.size()
	}

	offStart := pos
	pos += val.OffsetsSize(len(items))
	offStop := pos

	pos += itemCountSize
	pos += treeLevelSize

	nd = pool.Get(uint64(pos))

	writeItemCount(nd, len(items))
	writeTreeLevel(nd, level)

	pos = 0
	offs := val.Offsets(nd[offStart:offStop])
	for i, item := range items {
		copy(nd[pos:pos+item.size()], item)
		offs.Put(i, pos)
		pos += item.size()
	}

	return nd
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

func (nd node) count() int {
	stop := nd.size() - treeLevelSize
	start := stop - itemCountSize
	return int(binary.LittleEndian.Uint16(nd[start:stop]))
}

func (nd node) offsets() (offs val.Offsets, itemStop val.ByteSize) {
	stop := nd.size() - treeLevelSize - itemCountSize
	itemStop = stop - val.OffsetsSize(nd.count())
	return val.Offsets(nd[itemStop:stop]), itemStop
}

func (nd node) leafNode() bool {
	return nd.level() == 0
}

func writeTreeLevel(nd node, level uint64) {
	nd[nd.size()-treeLevelSize] = uint8(level)
}

func writeItemCount(nd node, count int) {
	stop := nd.size() - treeLevelSize
	start := stop - itemCountSize
	binary.LittleEndian.PutUint16(nd[start:stop], uint16(count))
}
