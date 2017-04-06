// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

func newCompactingChunkSource(mt *memTable, haver chunkReader, p tablePersister, rl chan struct{}) *compactingChunkSource {
	ccs := &compactingChunkSource{mt: mt}
	ccs.wg.Add(1)
	rl <- struct{}{}
	go func() {
		defer ccs.wg.Done()
		cs := p.Compact(mt, haver)

		ccs.mu.Lock()
		defer ccs.mu.Unlock()
		ccs.cs = cs
		ccs.mt = nil
		<-rl
	}()
	return ccs
}

type compactingChunkSource struct {
	mu sync.RWMutex
	mt *memTable

	wg sync.WaitGroup
	cs chunkSource
}

func (ccs *compactingChunkSource) getReader() chunkReader {
	ccs.mu.RLock()
	defer ccs.mu.RUnlock()
	if ccs.mt != nil {
		return ccs.mt
	}
	return ccs.cs
}

func (ccs *compactingChunkSource) has(h addr) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.has(h)
}

func (ccs *compactingChunkSource) hasMany(addrs []hasRecord) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.hasMany(addrs)
}

func (ccs *compactingChunkSource) get(h addr) []byte {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.get(h)
}

func (ccs *compactingChunkSource) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup) bool {
	cr := ccs.getReader()
	d.Chk.True(cr != nil)
	return cr.getMany(reqs, foundChunks, wg)
}

func (ccs *compactingChunkSource) close() error {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.close()
}

func (ccs *compactingChunkSource) count() uint32 {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.count()
}

func (ccs *compactingChunkSource) uncompressedLen() uint64 {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.uncompressedLen()
}

func (ccs *compactingChunkSource) hash() addr {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.hash()
}

func (ccs *compactingChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool) {
	ccs.wg.Wait()
	d.Chk.True(ccs.cs != nil)
	return ccs.cs.calcReads(reqs, blockSize)
}

func (ccs *compactingChunkSource) extract(chunks chan<- extractRecord) {
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

func (ecs emptyChunkSource) get(h addr) []byte {
	return nil
}

func (ecs emptyChunkSource) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup) bool {
	return true
}

func (ecs emptyChunkSource) close() error {
	return nil
}

func (ecs emptyChunkSource) count() uint32 {
	return 0
}

func (ecs emptyChunkSource) uncompressedLen() uint64 {
	return 0
}

func (ecs emptyChunkSource) hash() addr {
	return addr{} // TODO: is this legal?
}

func (ecs emptyChunkSource) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool) {
	return 0, true
}

func (ecs emptyChunkSource) extract(chunks chan<- extractRecord) {}
