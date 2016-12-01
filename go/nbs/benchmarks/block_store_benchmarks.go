// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

type blockStore interface {
	types.BatchStore
	GetMany(hashes []hash.Hash) []chunks.Chunk
}

type storeOpenFn func() blockStore

func benchmarkNovelWrite(refreshStore storeOpenFn, src *dataSource, t assert.TestingT) bool {
	store := refreshStore()
	writeToEmptyStore(store, src, t)
	assert.NoError(t, store.Close())
	return true
}

func writeToEmptyStore(store blockStore, src *dataSource, t assert.TestingT) {
	root := store.Root()
	assert.Equal(t, hash.Hash{}, root)

	chunx := goReadChunks(src)
	for c := range chunx {
		store.SchedulePut(*c, 1, types.Hints{})
	}
	newRoot := chunks.NewChunk([]byte("root"))
	store.SchedulePut(newRoot, 1, types.Hints{})
	assert.True(t, store.UpdateRoot(newRoot.Hash(), root))
}

func goReadChunks(src *dataSource) <-chan *chunks.Chunk {
	chunx := make(chan *chunks.Chunk, 1024)
	go func() {
		src.ReadChunks(chunx)
		close(chunx)
	}()
	return chunx
}

func benchmarkNoRefreshWrite(openStore storeOpenFn, src *dataSource, t assert.TestingT) {
	store := openStore()
	chunx := goReadChunks(src)
	for c := range chunx {
		store.SchedulePut(*c, 1, types.Hints{})
	}
	assert.NoError(t, store.Close())
}

func verifyChunk(h hash.Hash, c chunks.Chunk) {
	if len(c.Data()) == 0 {
		panic(fmt.Sprintf("Failed to fetch %s\n", h.String()))
	}
}

func benchmarkRead(openStore storeOpenFn, hashes hashSlice, src *dataSource, t assert.TestingT) {
	store := openStore()
	for _, h := range hashes {
		verifyChunk(h, store.Get(h))
	}
	assert.NoError(t, store.Close())
}

func verifyChunks(hashes []hash.Hash, batch []chunks.Chunk) {
	for i, c := range batch {
		verifyChunk(hashes[i], c)
	}
}

func benchmarkReadMany(openStore storeOpenFn, hashes hashSlice, src *dataSource, batchSize, concurrency int, t assert.TestingT) {
	store := openStore()
	batch := make([]hash.Hash, 0, batchSize)

	wg := sync.WaitGroup{}
	limit := make(chan struct{}, concurrency)

	for _, h := range hashes {
		batch = append(batch, h)

		if len(batch) == batchSize {
			limit <- struct{}{}
			wg.Add(1)
			go func(hashes []hash.Hash) {
				verifyChunks(hashes, store.GetMany(hashes))
				wg.Done()
				<-limit
			}(batch)

			batch = make([]hash.Hash, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		verifyChunks(batch, store.GetMany(batch))
	}

	wg.Wait()

	assert.NoError(t, store.Close())
}

func ensureNovelWrite(wrote bool, openStore storeOpenFn, src *dataSource, t assert.TestingT) bool {
	if !wrote {
		store := openStore()
		defer store.Close()
		writeToEmptyStore(store, src, t)
	}
	return true
}
