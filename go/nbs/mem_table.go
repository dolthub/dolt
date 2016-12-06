// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import "sort"

type memTable struct {
	chunks             map[addr][]byte
	order              []hasRecord
	maxData, totalData uint64
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

func (mt *memTable) get(h addr) []byte {
	return mt.chunks[h]
}

func (mt *memTable) getMany(reqs []getRecord) (remaining bool) {
	for i, r := range reqs {
		data := mt.chunks[*r.a]
		if data != nil {
			reqs[i].data = data
		} else {
			remaining = true
		}
	}
	return
}

func (mt *memTable) write(haver chunkReader) (name addr, data []byte, count uint32) {
	maxSize := maxTableSize(uint64(len(mt.order)), mt.totalData)
	buff := make([]byte, maxSize)
	tw := newTableWriter(buff)

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
	return name, buff[:tableSize], count
}
