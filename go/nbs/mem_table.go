// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
)

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

func (mt *memTable) count() uint32 {
	return uint32(len(mt.order))
}

func (mt *memTable) uncompressedLen() uint64 {
	return mt.totalData
}

func (mt *memTable) has(h addr) (has bool) {
	_, has = mt.chunks[h]
	return
}

func (mt *memTable) hasMany(addrs []hasRecord) (remaining bool) {
	for i, addr := range addrs {
		if addr.has {
			continue
		}

		if mt.has(*addr.a) {
			addrs[i].has = true
		} else {
			remaining = true
		}
	}
	return
}

func (mt *memTable) get(h addr, stats *Stats) []byte {
	return mt.chunks[h]
}

func (mt *memTable) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, stats *Stats) (remaining bool) {
	for _, r := range reqs {
		data := mt.chunks[*r.a]
		if data != nil {
			c := chunks.NewChunkWithHash(hash.Hash(*r.a), data)
			foundChunks <- &c
		} else {
			remaining = true
		}
	}
	return
}

func (mt *memTable) extract(chunks chan<- extractRecord) {
	for _, hrec := range mt.order {
		chunks <- extractRecord{a: *hrec.a, data: mt.chunks[*hrec.a]}
	}
	return
}

func (mt *memTable) write(haver chunkReader, stats *Stats) (name addr, data []byte, count uint32) {
	maxSize := maxTableSize(uint64(len(mt.order)), mt.totalData)
	buff := make([]byte, maxSize)
	tw := newTableWriter(buff, mt.snapper)

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		haver.hasMany(mt.order)
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	for _, addr := range mt.order {
		if !addr.has {
			h := addr.a
			tw.addChunk(*h, mt.chunks[*h])
			count++
		}
	}
	tableSize, name := tw.finish()

	if count > 0 {
		stats.BytesPerPersist.Sample(uint64(tableSize))
		stats.CompressedChunkBytesPerPersist.Sample(uint64(tw.totalCompressedData))
		stats.UncompressedChunkBytesPerPersist.Sample(uint64(tw.totalUncompressedData))
		stats.ChunksPerPersist.Sample(uint64(count))
	}

	return name, buff[:tableSize], count
}
