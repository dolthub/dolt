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

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type storeOpenFn func() (chunks.ChunkStore, error)

func benchmarkNovelWrite(refreshStore storeOpenFn, src *dataSource, t assert.TestingT) bool {
	store, err := refreshStore()
	assert.NoError(t, err)
	writeToEmptyStore(store, src, t)
	assert.NoError(t, store.Close())
	return true
}

func writeToEmptyStore(store chunks.ChunkStore, src *dataSource, t assert.TestingT) {
	root, err := store.Root(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, hash.Hash{}, root)

	chunx := goReadChunks(src)
	for c := range chunx {
		err := store.Put(context.Background(), *c)
		assert.NoError(t, err)
	}
	newRoot := chunks.NewChunk([]byte("root"))
	err = store.Put(context.Background(), newRoot)
	assert.NoError(t, err)
	success, err := store.Commit(context.Background(), newRoot.Hash(), root)
	assert.NoError(t, err)
	assert.True(t, success)
}

func goReadChunks(src *dataSource) <-chan *chunks.Chunk {
	chunx := make(chan *chunks.Chunk, 1024)
	go func() {
		err := src.ReadChunks(chunx)

		d.PanicIfError(err)

		close(chunx)
	}()
	return chunx
}

func benchmarkNoRefreshWrite(openStore storeOpenFn, src *dataSource, t assert.TestingT) {
	store, err := openStore()
	assert.NoError(t, err)
	chunx := goReadChunks(src)
	for c := range chunx {
		err := store.Put(context.Background(), *c)
		assert.NoError(t, err)
	}
	assert.NoError(t, store.Close())
}

func verifyChunk(h hash.Hash, c chunks.Chunk) {
	if len(c.Data()) == 0 {
		panic(fmt.Sprintf("Failed to fetch %s\n", h.String()))
	}
}

func benchmarkRead(openStore storeOpenFn, hashes hashSlice, src *dataSource, t assert.TestingT) {
	store, err := openStore()
	assert.NoError(t, err)
	for _, h := range hashes {
		c, err := store.Get(context.Background(), h)
		assert.NoError(t, err)
		verifyChunk(h, c)
	}
	assert.NoError(t, store.Close())
}

func verifyChunks(hashes hash.HashSlice, foundChunks chan *chunks.Chunk) {
	requested := hashes.HashSet()

	for c := range foundChunks {
		if _, ok := requested[c.Hash()]; !ok {
			panic(fmt.Sprintf("Got unexpected chunk: %s", c.Hash().String()))
		}

		delete(requested, c.Hash())
	}

	if len(requested) > 0 {
		for h := range requested {
			fmt.Printf("Failed to fetch %s\n", h.String())
		}
		panic("failed to fetch chunks")
	}
}

func benchmarkReadMany(openStore storeOpenFn, hashes hashSlice, src *dataSource, batchSize, concurrency int, t assert.TestingT) {
	store, err := openStore()
	assert.NoError(t, err)

	batch := make(hash.HashSlice, 0, batchSize)

	wg := sync.WaitGroup{}
	limit := make(chan struct{}, concurrency)

	for _, h := range hashes {
		batch = append(batch, h)

		if len(batch) == batchSize {
			limit <- struct{}{}
			wg.Add(1)
			go func(hashes hash.HashSlice) {
				chunkChan := make(chan *chunks.Chunk, len(hashes))
				err := store.GetMany(context.Background(), hashes.HashSet(), chunkChan)

				d.PanicIfError(err)

				close(chunkChan)
				verifyChunks(hashes, chunkChan)
				wg.Done()
				<-limit
			}(batch)

			batch = make([]hash.Hash, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		chunkChan := make(chan *chunks.Chunk, len(batch))
		err := store.GetMany(context.Background(), batch.HashSet(), chunkChan)
		assert.NoError(t, err)

		close(chunkChan)

		verifyChunks(batch, chunkChan)
	}

	wg.Wait()

	assert.NoError(t, store.Close())
}

func ensureNovelWrite(wrote bool, openStore storeOpenFn, src *dataSource, t assert.TestingT) bool {
	if !wrote {
		store, err := openStore()
		assert.NoError(t, err)
		defer store.Close()
		writeToEmptyStore(store, src, t)
	}
	return true
}
