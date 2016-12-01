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
	"github.com/attic-labs/noms/go/types"
	"github.com/dustin/go-humanize"
)

type fileBlockStore struct {
	bw *bufio.Writer
	w  io.WriteCloser
}

func newFileBlockStore(w io.WriteCloser) blockStore {
	return fileBlockStore{bufio.NewWriterSize(w, humanize.MiByte), w}
}

func (fb fileBlockStore) Get(h hash.Hash) chunks.Chunk {
	panic("not impl")
}

func (fb fileBlockStore) GetMany(batch []hash.Hash) (result []chunks.Chunk) {
	panic("not impl")
}

func (fb fileBlockStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
	io.Copy(fb.bw, bytes.NewReader(c.Data()))
}

func (fb fileBlockStore) AddHints(hints types.Hints) {
}

func (fb fileBlockStore) Flush() {}

func (fb fileBlockStore) Close() error {
	fb.w.Close()
	return nil
}

func (fb fileBlockStore) Root() hash.Hash {
	return hash.Hash{}
}

func (fb fileBlockStore) UpdateRoot(current, last hash.Hash) bool {
	fb.bw.Flush()
	return true
}
