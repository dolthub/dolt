// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

func newPersistingChunkSource(mt *memTable, haver chunkReader, p tablePersister, rl chan struct{}, stats *Stats) *persistingChunkSource {
	t1 := time.Now()

	ccs := &persistingChunkSource{mt: mt}
	ccs.wg.Add(1)
	rl <- struct{}{}
	go func() {
		defer ccs.wg.Done()
		cs := p.Persist(mt, haver, stats)

		ccs.mu.Lock()
		defer ccs.mu.Unlock()
		ccs.cs = cs
		ccs.mt = nil
		<-rl

		if cs.count() > 0 {
			stats.PersistLatency.SampleTimeSince(t1)
		}
	}()
	return ccs
}

type persistingChunkSource struct {
	mu sync.RWMutex
	mt *memTable

	wg sync.WaitGroup
	cs chunkSource
}

func (ccs *persistingChunkSource) getReader() chunkReader {
	ccs.mu.RLock()
	defer ccs.mu.RUnlock()
	if ccs.mt != nil {
		return ccs.mt
	}
	return ccs.cs
}

func (ccs *persistingChunkSource) has(h addr) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.has(h)
}

func (ccs *persistingChunkSource) hasMany(addrs []hasRecord) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.hasMany(addrs)
}

func (ccs *persistingChunkSource) get(h addr, stats *Stats) []byte {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.get(h, stats)
}

func (ccs *persistingChunkSource) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, stats *Stats) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.getMany(reqs, foundChunks, wg, stats)
}

func (ccs *persistingChunkSource) count() uint32 {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.count()
}

func (ccs *persistingChunkSource) uncompressedLen() uint64 {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.uncompressedLen()
}

func (ccs *persistingChunkSource) hash() addr {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.hash()
}

func (ccs *persistingChunkSource) index() tableIndex {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.index()
}

func (ccs *persistingChunkSource) reader() io.Reader {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.reader()
}

func (ccs *persistingChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool) {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.calcReads(reqs, blockSize)
}

func (ccs *persistingChunkSource) extract(chunks chan<- extractRecord) {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	ccs.cs.extract(chunks)
}

type emptyChunkSource struct{}

func (ecs emptyChunkSource) has(h addr) bool {
	return false
}

func (ecs emptyChunkSource) hasMany(addrs []hasRecord) bool {
	return true
}

func (ecs emptyChunkSource) get(h addr, stats *Stats) []byte {
	return nil
}

func (ecs emptyChunkSource) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, stats *Stats) bool {
	return true
}

func (ecs emptyChunkSource) count() uint32 {
	return 0
}

func (ecs emptyChunkSource) uncompressedLen() uint64 {
	return 0
}

func (ecs emptyChunkSource) hash() addr {
	return addr{}
}

func (ecs emptyChunkSource) index() tableIndex {
	return tableIndex{}
}

func (ecs emptyChunkSource) reader() io.Reader {
	return &bytes.Buffer{}
}

func (ecs emptyChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool) {
	return 0, true
}

func (ecs emptyChunkSource) extract(chunks chan<- extractRecord) {}
