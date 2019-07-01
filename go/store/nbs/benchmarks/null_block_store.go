// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type nullBlockStore struct {
}

func newNullBlockStore() (chunks.ChunkStore, error) {
	return nullBlockStore{}, nil
}

func (nb nullBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	panic("not impl")
}

func (nb nullBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *chunks.Chunk) error {
	panic("not impl")
}

func (nb nullBlockStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	panic("not impl")
}

func (nb nullBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (present hash.HashSet, err error) {
	panic("not impl")
}

func (nb nullBlockStore) Put(ctx context.Context, c chunks.Chunk) error {
	return nil
}

func (nb nullBlockStore) Version() string {
	panic("not impl")
}

func (nb nullBlockStore) Close() error {
	return nil
}

func (nb nullBlockStore) Rebase(ctx context.Context) error {
	return nil
}

func (nb nullBlockStore) Stats() interface{} {
	return nil
}

func (nb nullBlockStore) StatsSummary() string {
	return "Unsupported"
}

func (nb nullBlockStore) Root(ctx context.Context) (hash.Hash, error) {
	return hash.Hash{}, nil
}

func (nb nullBlockStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return true, nil
}
