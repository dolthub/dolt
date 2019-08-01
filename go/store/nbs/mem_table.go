// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"sort"
	"sync"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func WriteChunks(chunks []chunks.Chunk) (string, []byte, error) {
	var size uint64
	for _, chunk := range chunks {
		size += uint64(len(chunk.Data()))
	}

	mt := newMemTable(size)

	for _, chunk := range chunks {
		if !mt.addChunk(addr(chunk.Hash()), chunk.Data()) {
			return "", nil, errors.New("didn't create this memory table with enough space to add all the chunks")
		}
	}

	var stats Stats
	name, data, count, err := mt.write(nil, &stats)

	if err != nil {
		return "", nil, err
	}

	if count != uint32(len(chunks)) {
		return "", nil, errors.New("didn't write everything")
	}

	return name.String(), data, nil
}

type memTable struct {
	chunks             map[addr][]byte
	order              []hasRecord // Must maintain the invariant that these are sorted by rec.order
	maxData, totalData uint64

	snapper snappyEncoder
}

func newMemTable(memTableSize uint64) *memTable {
	return &memTable{chunks: map[addr][]byte{}, maxData: memTableSize}
}

func (mt *memTable) addChunk(h addr, data []byte) bool {
	if len(data) == 0 {
		panic("NBS blocks cannont be zero length")
	}
	if _, ok := mt.chunks[h]; ok {
		return true
	}
	dataLen := uint64(len(data))
	if mt.totalData+dataLen > mt.maxData {
		return false
	}
	mt.totalData += dataLen
	mt.chunks[h] = data
	mt.order = append(mt.order, hasRecord{
		&h,
		h.Prefix(),
		len(mt.order),
		false,
	})
	return true
}

func (mt *memTable) count() (uint32, error) {
	return uint32(len(mt.order)), nil
}

func (mt *memTable) uncompressedLen() (uint64, error) {
	return mt.totalData, nil
}

func (mt *memTable) has(h addr) (bool, error) {
	_, has := mt.chunks[h]
	return has, nil
}

func (mt *memTable) hasMany(addrs []hasRecord) (bool, error) {
	var remaining bool
	for i, addr := range addrs {
		if addr.has {
			continue
		}

		ok, err := mt.has(*addr.a)

		if err != nil {
			return false, err
		}

		if ok {
			addrs[i].has = true
		} else {
			remaining = true
		}
	}
	return remaining, nil
}

func (mt *memTable) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	return mt.chunks[h], nil
}

func (mt *memTable) getMany(ctx context.Context, reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
	var remaining bool
	for _, r := range reqs {
		data := mt.chunks[*r.a]
		if data != nil {
			c := chunks.NewChunkWithHash(hash.Hash(*r.a), data)
			foundChunks <- &c
		} else {
			remaining = true
		}
	}

	return remaining
}

func (mt *memTable) extract(ctx context.Context, chunks chan<- extractRecord) error {
	for _, hrec := range mt.order {
		chunks <- extractRecord{a: *hrec.a, data: mt.chunks[*hrec.a], err: nil}
	}

	return nil
}

func (mt *memTable) write(haver chunkReader, stats *Stats) (name addr, data []byte, count uint32, err error) {
	maxSize := maxTableSize(uint64(len(mt.order)), mt.totalData)
	buff := make([]byte, maxSize)
	tw := newTableWriter(buff, mt.snapper)

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		_, err := haver.hasMany(mt.order)

		if err != nil {
			return addr{}, nil, 0, err
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
		return addr{}, nil, 0, err
	}

	if count > 0 {
		stats.BytesPerPersist.Sample(uint64(tableSize))
		stats.CompressedChunkBytesPerPersist.Sample(uint64(tw.totalCompressedData))
		stats.UncompressedChunkBytesPerPersist.Sample(uint64(tw.totalUncompressedData))
		stats.ChunksPerPersist.Sample(uint64(count))
	}

	return name, buff[:tableSize], count, nil
}
