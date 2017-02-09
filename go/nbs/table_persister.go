// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/sizecache"
)

type tablePersister interface {
	Compact(mt *memTable, haver chunkReader) chunkSource
	CompactAll(sources chunkSources) chunkSource
	Open(name addr, chunkCount uint32) chunkSource
}

type indexCache struct {
	cache *sizecache.SizeCache
}

// Returns an indexCache which will burn roughly |size| bytes of memory
func newIndexCache(size uint64) *indexCache {
	return &indexCache{sizecache.New(size)}
}

func (sic indexCache) get(name addr) (tableIndex, bool) {
	idx, found := sic.cache.Get(name)
	if found {
		return idx.(tableIndex), true
	}

	return tableIndex{}, false
}

func (sic indexCache) put(name addr, idx tableIndex) {
	indexSize := uint64(idx.chunkCount) * (addrSize + ordinalSize + lengthSize + uint64Size)
	sic.cache.Add(name, indexSize, idx)
}

type chunkSourcesByDescendingCount chunkSources

func (csbc chunkSourcesByDescendingCount) Len() int { return len(csbc) }
func (csbc chunkSourcesByDescendingCount) Less(i, j int) bool {
	srcI, srcJ := csbc[i], csbc[j]
	if srcI.count() == srcJ.count() {
		hi, hj := srcI.hash(), srcJ.hash()
		return bytes.Compare(hi[:], hj[:]) > 0
	}
	return srcI.count() > srcJ.count()
}
func (csbc chunkSourcesByDescendingCount) Swap(i, j int) { csbc[i], csbc[j] = csbc[j], csbc[i] }

func compactSourcesToBuffer(sources chunkSources, rl chan struct{}) (name addr, data []byte, chunkCount uint32) {
	d.Chk.True(rl != nil)
	totalData := uint64(0)
	for _, src := range sources {
		chunkCount += src.count()
		totalData += src.byteLen()
	}
	if chunkCount == 0 {
		return
	}

	maxSize := maxTableSize(uint64(chunkCount), totalData)
	buff := make([]byte, maxSize) // This can blow up RAM (BUG 3130)
	tw := newTableWriter(buff, nil)

	// Use "channel of channels" ordered-concurrency pattern so that chunks from a given table stay together, preserving whatever locality was present in that table.
	chunkChans := make(chan chan extractRecord)
	type errRec struct {
		name addr
		err  interface{}
	}
	// TODO: remove/clean up this error reporting. BUG 3148
	errChan := make(chan errRec, len(sources)) // This way we don't have to worry about sends on errChan ever blocking
	go func() {
		defer close(chunkChans)
		defer close(errChan)
		wg := sync.WaitGroup{}
		for _, src := range sources {
			chunks := make(chan extractRecord)
			wg.Add(1)
			go func(s chunkSource, c chan<- extractRecord) {
				defer func() { close(c); wg.Done(); <-rl }()
				defer func() {
					if r := recover(); r != nil {
						errChan <- errRec{s.hash(), r}
					}
				}()
				rl <- struct{}{}

				s.extract(InsertOrder, c)
			}(src, chunks)
			chunkChans <- chunks
		}
		wg.Wait()
	}()

	known := map[addr]struct{}{}
	for chunks := range chunkChans {
		for chunk := range chunks {
			if _, present := known[chunk.a]; !present {
				tw.addChunk(chunk.a, chunk.data)
				known[chunk.a] = struct{}{}
			}
		}
	}

	errString := ""
	for e := range errChan {
		errString += fmt.Sprintf("Failed to extract %s:\n %v\n******\n\n", e.name, e.err)
	}
	if errString != "" {
		panic(fmt.Errorf(errString))
	}

	tableSize, name := tw.finish()
	return name, buff[:tableSize], uint32(len(known))
}
