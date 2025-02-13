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

package nbs

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type addChunkResult int

const (
	chunkExists addChunkResult = iota
	chunkAdded
	chunkNotAdded
)

func WriteChunks(chunks []chunks.Chunk) (string, []byte, error) {
	var size uint64
	for _, chunk := range chunks {
		size += uint64(len(chunk.Data()))
	}

	mt := newMemTable(size)

	return writeChunksToMT(mt, chunks)
}

func writeChunksToMT(mt *memTable, chunks []chunks.Chunk) (string, []byte, error) {
	for _, chunk := range chunks {
		res := mt.addChunk(chunk.Hash(), chunk.Data())
		if res == chunkNotAdded {
			return "", nil, errors.New("didn't create this memory table with enough space to add all the chunks")
		}
	}

	var stats Stats
	name, data, count, _, err := mt.write(nil, nil, &stats)

	if err != nil {
		return "", nil, err
	}

	if count != uint32(len(chunks)) {
		return "", nil, errors.New("didn't write everything")
	}

	return name.String(), data, nil
}

type memTable struct {
	chunks             map[hash.Hash][]byte
	order              []hasRecord // Must maintain the invariant that these are sorted by rec.order
	pendingRefs        []hasRecord
	getChildAddrs      []chunks.GetAddrsCb
	maxData, totalData uint64

	snapper snappyEncoder
}

func newMemTable(memTableSize uint64) *memTable {
	return &memTable{chunks: map[hash.Hash][]byte{}, maxData: memTableSize}
}

func (mt *memTable) addChunk(h hash.Hash, data []byte) addChunkResult {
	if len(data) == 0 {
		panic("NBS blocks cannot be zero length")
	}
	if _, ok := mt.chunks[h]; ok {
		return chunkExists
	}

	dataLen := uint64(len(data))
	if mt.totalData+dataLen > mt.maxData {
		return chunkNotAdded
	}

	mt.totalData += dataLen
	mt.chunks[h] = data
	mt.order = append(mt.order, hasRecord{
		&h,
		h.Prefix(),
		len(mt.order),
		false,
	})
	return chunkAdded
}

func (mt *memTable) addGetChildRefs(getAddrs chunks.GetAddrsCb) {
	mt.getChildAddrs = append(mt.getChildAddrs, getAddrs)
}

func (mt *memTable) addChildRefs(addrs hash.HashSet) {
	for h := range addrs {
		h := h
		mt.pendingRefs = append(mt.pendingRefs, hasRecord{
			a:      &h,
			prefix: h.Prefix(),
			order:  len(mt.pendingRefs),
		})
	}
}

func (mt *memTable) count() (uint32, error) {
	return uint32(len(mt.order)), nil
}

func (mt *memTable) uncompressedLen() (uint64, error) {
	return mt.totalData, nil
}

func (mt *memTable) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	_, has := mt.chunks[h]
	if has && keeper != nil && keeper(h) {
		return false, gcBehavior_Block, nil
	}
	return has, gcBehavior_Continue, nil
}

func (mt *memTable) hasMany(addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	var remaining bool
	for i, addr := range addrs {
		if addr.has {
			continue
		}

		ok, gcb, err := mt.has(*addr.a, keeper)
		if err != nil {
			return false, gcBehavior_Continue, err
		}
		if gcb != gcBehavior_Continue {
			return ok, gcb, nil
		}

		if ok {
			addrs[i].has = true
		} else {
			remaining = true
		}
	}
	return remaining, gcBehavior_Continue, nil
}

func (mt *memTable) get(ctx context.Context, h hash.Hash, keeper keeperF, stats *Stats) ([]byte, gcBehavior, error) {
	c, ok := mt.chunks[h]
	if ok && keeper != nil && keeper(h) {
		return nil, gcBehavior_Block, nil
	}
	return c, gcBehavior_Continue, nil
}

func (mt *memTable) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	var remaining bool
	for i, r := range reqs {
		data := mt.chunks[*r.a]
		if data != nil {
			if keeper != nil && keeper(*r.a) {
				return true, gcBehavior_Block, nil
			}
			c := chunks.NewChunkWithHash(hash.Hash(*r.a), data)
			reqs[i].found = true
			found(ctx, &c)
		} else {
			remaining = true
		}
	}
	return remaining, gcBehavior_Continue, nil
}

func (mt *memTable) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, ToChunker), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	var remaining bool
	for i, r := range reqs {
		data := mt.chunks[*r.a]
		if data != nil {
			if keeper != nil && keeper(*r.a) {
				return true, gcBehavior_Block, nil
			}
			c := chunks.NewChunkWithHash(hash.Hash(*r.a), data)
			reqs[i].found = true
			found(ctx, ChunkToCompressedChunk(c))
		} else {
			remaining = true
		}
	}

	return remaining, gcBehavior_Continue, nil
}

func (mt *memTable) extract(ctx context.Context, chunks chan<- extractRecord) error {
	for _, hrec := range mt.order {
		chunks <- extractRecord{a: *hrec.a, data: mt.chunks[*hrec.a], err: nil}
	}

	return nil
}

func (mt *memTable) write(haver chunkReader, keeper keeperF, stats *Stats) (name hash.Hash, data []byte, count uint32, gcb gcBehavior, err error) {
	gcb = gcBehavior_Continue
	numChunks := uint64(len(mt.order))
	if numChunks == 0 {
		return hash.Hash{}, nil, 0, gcBehavior_Continue, fmt.Errorf("mem table cannot write with zero chunks")
	}
	maxSize := maxTableSize(uint64(len(mt.order)), mt.totalData)
	// todo: memory quota
	buff := make([]byte, maxSize)
	tw := newTableWriter(buff, mt.snapper)

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		_, gcb, err = haver.hasMany(mt.order, keeper)
		if err != nil {
			return hash.Hash{}, nil, 0, gcBehavior_Continue, err
		}
		if gcb != gcBehavior_Continue {
			return hash.Hash{}, nil, 0, gcb, err
		}

		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	for _, addr := range mt.order {
		if !addr.has {
			h := addr.a
			tw.addChunk(*h, mt.chunks[*h])
			count++
		}
	}
	tableSize, name, err := tw.finish()

	if err != nil {
		return hash.Hash{}, nil, 0, gcBehavior_Continue, err
	}

	if count > 0 {
		stats.BytesPerPersist.Sample(uint64(tableSize))
		stats.CompressedChunkBytesPerPersist.Sample(uint64(tw.totalCompressedData))
		stats.UncompressedChunkBytesPerPersist.Sample(uint64(tw.totalUncompressedData))
		stats.ChunksPerPersist.Sample(uint64(count))
	}

	return name, buff[:tableSize], count, gcBehavior_Continue, nil
}

func (mt *memTable) close() error {
	return nil
}
