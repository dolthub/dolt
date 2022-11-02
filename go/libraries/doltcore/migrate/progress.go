// Copyright 2022 Dolthub, Inc.
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

package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type ChunkMapping interface {
	Has(ctx context.Context, addr hash.Hash) (bool, error)
	Get(ctx context.Context, addr hash.Hash) (hash.Hash, error)
	Put(ctx context.Context, old, new hash.Hash) error
	Close(ctx context.Context) error
}

type CommitStack interface {
	Push(ctx context.Context, cm *doltdb.Commit) error
	Pop(ctx context.Context) (*doltdb.Commit, error)
}

type Progress interface {
	ChunkMapping
	CommitStack

	Log(ctx context.Context, format string, args ...any)
	Close(ctx context.Context) error
}

// A memory stack with a persisted commit mapping.
type memoryStackProgress struct {
	stack    []*doltdb.Commit
	mapping  *prolly.MutableMap
	kb, vb   *val.TupleBuilder
	buffPool pool.BuffPool
	vs       *types.ValueStore
	cs       chunks.ChunkStore
}

func newProgress(ctx context.Context, cs chunks.ChunkStore) (Progress, error) {
	kd := val.NewTupleDescriptor(val.Type{
		Enc:      val.ByteStringEnc,
		Nullable: false,
	})
	vd := val.NewTupleDescriptor(val.Type{
		Enc:      val.ByteStringEnc,
		Nullable: false,
	})

	ns := tree.NewNodeStore(cs)
	vs := types.NewValueStore(cs)

	mapping, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	if err != nil {
		return nil, err
	}

	mut := mapping.Mutate()
	kb := val.NewTupleBuilder(kd)
	vb := val.NewTupleBuilder(vd)

	return &memoryStackProgress{
		stack:    make([]*doltdb.Commit, 0, 128),
		mapping:  mut,
		kb:       kb,
		vb:       vb,
		buffPool: ns.Pool(),
		vs:       vs,
		cs:       cs,
	}, nil
}

func (mem *memoryStackProgress) Has(ctx context.Context, addr hash.Hash) (ok bool, err error) {
	mem.kb.PutByteString(0, addr[:])
	k := mem.kb.Build(mem.buffPool)
	return mem.mapping.Has(ctx, k)
}

func (mem *memoryStackProgress) Get(ctx context.Context, old hash.Hash) (new hash.Hash, err error) {
	mem.kb.PutByteString(0, old[:])
	k := mem.kb.Build(mem.buffPool)
	err = mem.mapping.Get(ctx, k, func(_, v val.Tuple) error {
		if len(v) > 0 {
			n, ok := mem.vb.Desc.GetBytes(0, v)
			if !ok {
				return fmt.Errorf("failed to get string address from commit mapping value")
			}
			new = hash.New(n)
		}
		return nil
	})
	return
}

func (mem *memoryStackProgress) Put(ctx context.Context, old, new hash.Hash) (err error) {
	mem.kb.PutByteString(0, old[:])
	k := mem.kb.Build(mem.buffPool)
	mem.vb.PutByteString(0, new[:])
	v := mem.vb.Build(mem.buffPool)
	err = mem.mapping.Put(ctx, k, v)
	return
}

func (mem *memoryStackProgress) Push(ctx context.Context, cm *doltdb.Commit) (err error) {
	mem.stack = append(mem.stack, cm)
	return
}

func (mem *memoryStackProgress) Pop(ctx context.Context) (cm *doltdb.Commit, err error) {
	if len(mem.stack) == 0 {
		return nil, nil
	}
	top := len(mem.stack) - 1
	cm = mem.stack[top]
	mem.stack = mem.stack[:top]
	return
}

func (mem *memoryStackProgress) Log(ctx context.Context, format string, args ...any) {
	cli.Println(time.Now().UTC().String() + " " + fmt.Sprintf(format, args...))
}

func (mem *memoryStackProgress) Close(ctx context.Context) error {
	m, err := mem.mapping.Map(ctx)
	if err != nil {
		return err
	}
	v := shim.ValueFromMap(m)
	ref, err := mem.vs.WriteValue(ctx, v)
	if err != nil {
		return err
	}
	last, err := mem.vs.Root(ctx)
	if err != nil {
		return err
	}
	ok, err := mem.vs.Commit(ctx, last, last)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to commit, manifest swapped out beneath us")
	}

	mem.Log(ctx, "Wrote commit mapping!! [commit_mapping_ref: %s]", ref.TargetHash().String())
	return nil
}
