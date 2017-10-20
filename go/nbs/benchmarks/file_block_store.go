// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"bytes"
	"io"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/dustin/go-humanize"
)

type fileBlockStore struct {
	bw *bufio.Writer
	w  io.WriteCloser
}

func newFileBlockStore(w io.WriteCloser) chunks.ChunkStore {
	return fileBlockStore{bufio.NewWriterSize(w, humanize.MiByte), w}
}

func (fb fileBlockStore) Get(h hash.Hash) chunks.Chunk {
	panic("not impl")
}

func (fb fileBlockStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	panic("not impl")
}

func (fb fileBlockStore) Has(h hash.Hash) bool {
	panic("not impl")
}

func (fb fileBlockStore) HasMany(hashes hash.HashSet) (present hash.HashSet) {
	panic("not impl")
}

func (fb fileBlockStore) Put(c chunks.Chunk) {
	io.Copy(fb.bw, bytes.NewReader(c.Data()))
}

func (fb fileBlockStore) Version() string {
	panic("not impl")
}

func (fb fileBlockStore) Close() error {
	fb.w.Close()
	return nil
}

func (fb fileBlockStore) Rebase() {}

func (fb fileBlockStore) Stats() interface{} {
	return nil
}

func (fb fileBlockStore) StatsSummary() string {
	return "Unsupported"
}

func (fb fileBlockStore) Root() hash.Hash {
	return hash.Hash{}
}

func (fb fileBlockStore) Commit(current, last hash.Hash) bool {
	fb.bw.Flush()
	return true
}
