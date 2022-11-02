// Copyright 2019 Dolthub, Inc.
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
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type fileBlockStore struct {
	bw *bufio.Writer
	w  io.WriteCloser
}

func newFileBlockStore(w io.WriteCloser) (chunks.ChunkStore, error) {
	return fileBlockStore{bufio.NewWriterSize(w, humanize.MiByte), w}, nil
}

func (fb fileBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	panic("not impl")
}

func (fb fileBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	panic("not impl")
}

func (fb fileBlockStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	panic("not impl")
}

func (fb fileBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (present hash.HashSet, err error) {
	panic("not impl")
}

func (fb fileBlockStore) Put(ctx context.Context, c chunks.Chunk) error {
	_, err := io.Copy(fb.bw, bytes.NewReader(c.Data()))
	return err
}

func (fb fileBlockStore) Version() string {
	panic("not impl")
}

func (fb fileBlockStore) Close() error {
	fb.w.Close()
	return nil
}

func (fb fileBlockStore) Rebase(ctx context.Context) error {
	return nil
}

func (fb fileBlockStore) Stats() interface{} {
	return nil
}

func (fb fileBlockStore) StatsSummary() string {
	return "Unsupported"
}

func (fb fileBlockStore) Root(ctx context.Context) (hash.Hash, error) {
	return hash.Hash{}, nil
}

func (fb fileBlockStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	err := fb.bw.Flush()
	return true, err
}
