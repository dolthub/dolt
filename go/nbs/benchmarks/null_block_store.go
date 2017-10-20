// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
)

type nullBlockStore struct {
	bogus int32
}

func newNullBlockStore() chunks.ChunkStore {
	return nullBlockStore{}
}

func (nb nullBlockStore) Get(h hash.Hash) chunks.Chunk {
	panic("not impl")
}

func (nb nullBlockStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	panic("not impl")
}

func (nb nullBlockStore) Has(h hash.Hash) bool {
	panic("not impl")
}

func (nb nullBlockStore) HasMany(hashes hash.HashSet) (present hash.HashSet) {
	panic("not impl")
}

func (nb nullBlockStore) Put(c chunks.Chunk) {}

func (nb nullBlockStore) Version() string {
	panic("not impl")
}

func (nb nullBlockStore) Close() error {
	return nil
}

func (nb nullBlockStore) Rebase() {}

func (nb nullBlockStore) Stats() interface{} {
	return nil
}

func (nb nullBlockStore) StatsSummary() string {
	return "Unsupported"
}

func (nb nullBlockStore) Root() hash.Hash {
	return hash.Hash{}
}

func (nb nullBlockStore) Commit(current, last hash.Hash) bool {
	return true
}
