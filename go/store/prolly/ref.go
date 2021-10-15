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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/chunks"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

type NodeReadWriter interface { // todo(andy): fun name
	Read(ctx context.Context, ref hash.Hash) (node, error)
	Write(ctx context.Context, nd node) (hash.Hash, error)
	Pool() pool.BuffPool
}

func fetchRef(ctx context.Context, nrw NodeReadWriter, item nodeItem) (node, error) {
	ref := val.Tuple(item).GetField(-1)
	if len(ref) != 20 {
		_ = val.Tuple(item).GetField(-1)
	}
	return nrw.Read(ctx, hash.New(ref))
}

// todo(andy): this is specific to Map
func writeNewNode(ctx context.Context, nrw NodeReadWriter, level uint64, items ...nodeItem) (node, metaTuple, error) {
	nd := makeProllyNode(nrw.Pool(), level, items...)
	leaf := level == 0

	h, err := nrw.Write(ctx, nd)
	if err != nil {
		return nil, nil, err
	}

	var last val.Tuple
	if leaf {
		last = val.Tuple(items[len(items)-2])
	} else {
		last = val.Tuple(items[len(items)-1])
	}

	var metaKey [][]byte
	for i := 0; i < last.Count(); i++ {
		metaKey = append(metaKey, last.GetField(i))
	}
	if !leaf {
		// discard ref from child
		metaKey = metaKey[:len(metaKey)-1]
	}

	meta := newMetaTuple(nrw.Pool(), h, metaKey)
	return nd, meta, nil
}

type metaTuple val.Tuple

func newMetaTuple(pool pool.BuffPool, ref hash.Hash, key [][]byte) metaTuple {
	key = append(key, ref[:])
	return metaTuple(val.NewTuple(pool, key...))
}

func (mt metaTuple) GetRef() hash.Hash {
	ref := val.Tuple(mt).GetField(-1)
	if len(ref) != 20 {
		panic("")
	}
	return hash.New(ref)
}

type nodeStore struct {
	store chunks.ChunkStore
	bp    pool.BuffPool
}

var _ NodeReadWriter = nodeStore{}

var sharedPool = pool.NewBuffPool()

func NewNodeStore(cs chunks.ChunkStore) NodeReadWriter {
	return nodeStore{store: cs, bp: sharedPool}
}

func (ns nodeStore) Read(ctx context.Context, ref hash.Hash) (node, error) {
	c, err := ns.store.Get(ctx, ref)
	return c.Data(), err
}

func (ns nodeStore) Write(ctx context.Context, nd node) (hash.Hash, error) {
	c := chunks.NewChunk(nd)
	err := ns.store.Put(ctx, c)
	return c.Hash(), err
}

func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}
