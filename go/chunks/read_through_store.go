// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// ReadThroughStore is a store that consists of two other stores. A caching and
// a backing store. All reads check the caching store first and if the h is
// present there the caching store is used. If not present the backing store is
// used and the value gets cached in the caching store. All writes go directly
// to the backing store.
type ReadThroughStore struct {
	io.Closer
	cachingStore ChunkStore
	backingStore ChunkStore
	putCount     int
}

func NewReadThroughStore(cachingStore ChunkStore, backingStore ChunkStore) ReadThroughStore {
	return ReadThroughStore{ioutil.NopCloser(nil), cachingStore, backingStore, 0}
}

func (rts ReadThroughStore) Get(h hash.Hash) Chunk {
	c := rts.cachingStore.Get(h)
	if !c.IsEmpty() {
		return c
	}
	c = rts.backingStore.Get(h)
	if c.IsEmpty() {
		return c
	}

	rts.cachingStore.Put(c)
	return c
}

func (rts ReadThroughStore) Has(h hash.Hash) bool {
	return rts.cachingStore.Has(h) || rts.backingStore.Has(h)
}

func (rts ReadThroughStore) Put(c Chunk) {
	rts.backingStore.Put(c)
	rts.cachingStore.Put(c)
}

func (rts ReadThroughStore) PutMany(chunks []Chunk) BackpressureError {
	bpe := rts.backingStore.PutMany(chunks)
	lookup := make(map[hash.Hash]bool, len(bpe))
	for _, r := range bpe {
		lookup[r] = true
	}
	toPut := make([]Chunk, 0, len(chunks)-len(bpe))
	for _, c := range chunks {
		if lookup[c.Hash()] {
			toPut = append(toPut, c)
		}
	}
	d.Chk.NoError(rts.cachingStore.PutMany(toPut))
	return bpe
}

func (rts ReadThroughStore) Root() hash.Hash {
	return rts.backingStore.Root()
}

func (rts ReadThroughStore) UpdateRoot(current, last hash.Hash) bool {
	return rts.backingStore.UpdateRoot(current, last)
}

func (rts ReadThroughStore) Version() string {
	return rts.backingStore.Version()
}
