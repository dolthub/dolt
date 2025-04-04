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
	"io"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"

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

const (
	MigratedCommitsBranch = "dolt_migrated_commits"
	MigratedCommitsTable  = "dolt_commit_mapping"
)

var (
	mappingSchema, _ = schema.SchemaFromCols(schema.NewColCollection(
		schema.NewColumn("old_commit_hash", 0, types.StringKind, true),
		schema.NewColumn("new_commit_hash", 1, types.StringKind, false),
	))
	desc = val.NewTupleDescriptor(val.Type{Enc: val.StringEnc, Nullable: false})
)

// progress maintains the state of migration.
type progress struct {
	stack []*doltdb.Commit

	// mapping tracks migrated commits
	// it maps old commit hash to new hash
	mapping  *prolly.MutableMap
	kb, vb   *val.TupleBuilder
	buffPool pool.BuffPool

	vs *types.ValueStore
	cs chunks.ChunkStore
}

func newProgress(ctx context.Context, cs chunks.ChunkStore) (*progress, error) {
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

	return &progress{
		stack:    make([]*doltdb.Commit, 0, 128),
		mapping:  mut,
		kb:       kb,
		vb:       vb,
		buffPool: ns.Pool(),
		vs:       vs,
		cs:       cs,
	}, nil
}

func (p *progress) Has(ctx context.Context, addr hash.Hash) (ok bool, err error) {
	p.kb.PutByteString(0, addr[:])
	k := p.kb.Build(p.buffPool)
	return p.mapping.Has(ctx, k)
}

func (p *progress) Get(ctx context.Context, old hash.Hash) (new hash.Hash, err error) {
	p.kb.PutByteString(0, old[:])
	k := p.kb.Build(p.buffPool)
	err = p.mapping.Get(ctx, k, func(_, v val.Tuple) error {
		if len(v) > 0 {
			n, ok := p.vb.Desc.GetBytes(0, v)
			if !ok {
				return fmt.Errorf("failed to get string address from commit mapping value")
			}
			new = hash.New(n)
		}
		return nil
	})
	return
}

func (p *progress) Put(ctx context.Context, old, new hash.Hash) (err error) {
	p.kb.PutByteString(0, old[:])
	k := p.kb.Build(p.buffPool)
	p.vb.PutByteString(0, new[:])
	v := p.vb.Build(p.buffPool)
	err = p.mapping.Put(ctx, k, v)
	return
}

func (p *progress) Push(ctx context.Context, cm *doltdb.Commit) (err error) {
	p.stack = append(p.stack, cm)
	return
}

func (p *progress) Pop(ctx context.Context) (cm *doltdb.Commit, err error) {
	if len(p.stack) == 0 {
		return nil, nil
	}
	top := len(p.stack) - 1
	cm = p.stack[top]
	p.stack = p.stack[:top]
	return
}

func (p *progress) Log(ctx context.Context, format string, args ...any) {
	cli.Println(time.Now().UTC().String() + " " + fmt.Sprintf(format, args...))
}

func (p *progress) Finalize(ctx context.Context) (prolly.Map, error) {
	m, err := p.mapping.Map(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	v := shim.ValueFromMap(m)
	ref, err := p.vs.WriteValue(ctx, v)
	if err != nil {
		return prolly.Map{}, err
	}
	last, err := p.vs.Root(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	ok, err := p.vs.Commit(ctx, last, last)
	if err != nil {
		return prolly.Map{}, err
	} else if !ok {
		return prolly.Map{}, fmt.Errorf("failed to commit, manifest swapped out beneath us")
	}

	p.Log(ctx, "Wrote commit mapping!! [commit_mapping_ref: %s]", ref.TargetHash().String())
	p.Log(ctx, "Commit mapping allow mapping pre-migration commit hashes to post-migration commit hashes, "+
		"it is available on branch '%s' in table '%s'", MigratedCommitsBranch, MigratedCommitsTable)
	return m, nil
}

func persistMigratedCommitMapping(ctx context.Context, ddb *doltdb.DoltDB, mapping prolly.Map) error {
	// create a new branch to persist the migrated commit mapping
	init, err := ddb.ResolveCommitRef(ctx, ref.NewInternalRef(doltdb.CreationBranch))
	if err != nil {
		return err
	}

	br := ref.NewBranchRef(MigratedCommitsBranch)
	err = ddb.NewBranchAtCommit(ctx, br, init, nil)
	if err != nil {
		return err
	}

	ns, vrw := ddb.NodeStore(), ddb.ValueReadWriter()
	m, err := prolly.NewMapFromTuples(ctx, ns, desc, desc)
	if err != nil {
		return err
	}

	rows := m.Mutate()
	bld := val.NewTupleBuilder(desc)

	// convert |mapping| values from hash.Hash to string
	iter, err := mapping.IterAll(ctx)
	if err != nil {
		return err
	}

	var k, v val.Tuple
	kd, vd := mapping.Descriptors()
	for {
		k, v, err = iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		o, _ := kd.GetBytes(0, k)
		bld.PutString(0, hash.New(o).String())
		key := bld.Build(ddb.NodeStore().Pool())

		n, _ := vd.GetBytes(0, v)
		bld.PutString(0, hash.New(n).String())
		value := bld.Build(ddb.NodeStore().Pool())

		if err = rows.Put(ctx, key, value); err != nil {
			return err
		}
	}

	m, err = rows.Map(ctx)
	if err != nil {
		return err
	}
	idx := durable.IndexFromProllyMap(m)

	tbl, err := doltdb.NewTable(ctx, vrw, ns, mappingSchema, idx, nil, nil)
	if err != nil {
		return err
	}

	root, err := init.GetRootValue(ctx)
	if err != nil {
		return err
	}

	root, err = root.PutTable(ctx, doltdb.TableName{Name: MigratedCommitsTable}, tbl)
	if err != nil {
		return err
	}

	return commitRoot(ctx, ddb, br, root, init)
}

func commitRoot(
	ctx context.Context,
	ddb *doltdb.DoltDB,
	br ref.BranchRef,
	root doltdb.RootValue,
	parent *doltdb.Commit,
) error {
	roots := doltdb.Roots{
		Head:    root,
		Working: root,
		Staged:  root,
	}
	parents := []*doltdb.Commit{parent}

	meta, err := parent.GetCommitMeta(ctx)
	if err != nil {
		return err
	}

	meta, err = datas.NewCommitMeta(meta.Name, meta.Email, meta.Description)
	if err != nil {
		return err
	}

	pcm, err := ddb.NewPendingCommit(ctx, roots, parents, false, meta)
	if err != nil {
		return err
	}

	wsr, err := ref.WorkingSetRefForHead(br)
	if err != nil {
		return err
	}

	ws, err := ddb.ResolveWorkingSet(ctx, wsr)
	if err != nil {
		return err
	}

	prev, err := ws.HashOf()
	if err != nil {
		return err
	}
	ws = ws.WithWorkingRoot(root).WithStagedRoot(root)

	_, err = ddb.CommitWithWorkingSet(ctx, br, wsr, pcm, ws, prev, &datas.WorkingSetMeta{
		Name:      meta.Name,
		Email:     meta.Email,
		Timestamp: uint64(time.Now().Unix()),
	}, nil)
	return err
}
