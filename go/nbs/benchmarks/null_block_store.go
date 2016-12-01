// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type nullBlockStore struct {
	bogus int32
}

func newNullBlockStore() blockStore {
	return nullBlockStore{}
}

func (nb nullBlockStore) Get(h hash.Hash) chunks.Chunk {
	panic("not impl")
}

func (nb nullBlockStore) GetMany(batch []hash.Hash) (result []chunks.Chunk) {
	panic("not impl")
}

func (nb nullBlockStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
}

func (nb nullBlockStore) AddHints(hints types.Hints) {
}

func (nb nullBlockStore) Flush() {}

func (nb nullBlockStore) Close() error {
	return nil
}

func (nb nullBlockStore) Root() hash.Hash {
	return hash.Hash{}
}

func (nb nullBlockStore) UpdateRoot(current, last hash.Hash) bool {
	return true
}
