// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type fileBlockStore struct {
	bw *bufio.Writer
	w  io.WriteCloser
}

func newFileBlockStore(w io.WriteCloser) chunks.ChunkStore {
	return fileBlockStore{bufio.NewWriterSize(w, humanize.MiByte), w}
}

func (fb fileBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	panic("not impl")
}

func (fb fileBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *chunks.Chunk) error {
	panic("not impl")
}

func (fb fileBlockStore) Has(ctx context.Context, h hash.Hash) bool {
	panic("not impl")
}

func (fb fileBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (present hash.HashSet) {
	panic("not impl")
}

func (fb fileBlockStore) Put(ctx context.Context, c chunks.Chunk) {
	io.Copy(fb.bw, bytes.NewReader(c.Data()))
}

func (fb fileBlockStore) Version() string {
	panic("not impl")
}

func (fb fileBlockStore) Close() error {
	fb.w.Close()
	return nil
}

func (fb fileBlockStore) Rebase(ctx context.Context) {}

func (fb fileBlockStore) Stats() interface{} {
	return nil
}

func (fb fileBlockStore) StatsSummary() string {
	return "Unsupported"
}

func (fb fileBlockStore) Root(ctx context.Context) hash.Hash {
	return hash.Hash{}
}

func (fb fileBlockStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	fb.bw.Flush()
	return true, nil
}
