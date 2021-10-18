// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/chunks"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	metaTupleCountIdx = -2
	metaTupleRefIdx   = -1

	metaTupleRefSize = 20
)

type metaTuple val.Tuple

func newMetaTuple(pool pool.BuffPool, count uint64, ref hash.Hash, key [][]byte) metaTuple {
	var cnt [6]byte
	writeUint48(cnt[:], count)
	key = append(key, cnt[:], ref[:])
	return metaTuple(val.NewTuple(pool, key...))
}

func (mt metaTuple) GetCumulativeCount() uint64 {
	cnt := val.Tuple(mt).GetField(metaTupleCountIdx)
	return readUint48(cnt)
}

func (mt metaTuple) GetRef() hash.Hash {
	ref := val.Tuple(mt).GetField(metaTupleRefIdx)
	if len(ref) != metaTupleRefSize {
		panic("incorrect number of bytes for meta tuple ref")
	}
	return hash.New(ref)
}

func fetchRef(ctx context.Context, nrw NodeReadWriter, item nodeItem) (Node, error) {
	return nrw.Read(ctx, metaTuple(item).GetRef())
}

func writeNewNode(ctx context.Context, nrw NodeReadWriter, level uint64, items ...nodeItem) (Node, metaTuple, error) {
	nd := makeProllyNode(nrw.Pool(), level, items...)

	ref, err := nrw.Write(ctx, nd)
	if err != nil {
		return nil, nil, err
	}

	fields := metaTupleFields(level, items...)
	meta := newMetaTuple(nrw.Pool(), nd.cumulativeCount(), ref, fields)

	return nd, meta, nil
}

func metaTupleFields(level uint64, items ...nodeItem) (fields [][]byte) {
	// todo(andy): this is specific to Map
	var key val.Tuple
	var cnt int

	if level == 0 {
		key = val.Tuple(items[len(items)-2])
		cnt = key.Count()
	} else {
		key = val.Tuple(items[len(items)-1])
		// discard ref and count from child
		cnt = key.Count() - 2
	}

	for i := 0; i < cnt; i++ {
		fields = append(fields, key.GetField(i))
	}
	return
}

type NodeReadWriter interface { // todo(andy): fun name
	Read(ctx context.Context, ref hash.Hash) (Node, error)
	Write(ctx context.Context, nd Node) (hash.Hash, error)
	Pool() pool.BuffPool
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

func (ns nodeStore) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	c, err := ns.store.Get(ctx, ref)
	return c.Data(), err
}

func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd)
	err := ns.store.Put(ctx, c)
	return c.Hash(), err
}

func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}
