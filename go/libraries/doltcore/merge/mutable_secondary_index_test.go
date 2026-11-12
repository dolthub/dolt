// Copyright 2026 Dolthub, Inc.
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

package merge

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// partialIndexTestTable is a table t (id INT PRIMARY KEY, p INT, slot INT) with the partial unique index
// t_partial ON t (p) WHERE slot IS NULL.
type partialIndexTestTable struct {
	sch schema.Schema
	idx schema.Index
	vrw types.ValueReadWriter
	ns  tree.NodeStore
	bp  pool.BuffPool
}

func newPartialIndexTestTable(t *testing.T) partialIndexTestTable {
	ddb := dtestutils.CreateTestEnv().DoltDB(context.Background())
	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		mustCol(t, "id", 1, typeinfo.Int32Type, true),
		mustCol(t, "p", 2, typeinfo.Int32Type, false),
		mustCol(t, "slot", 3, typeinfo.Int32Type, false),
	))
	idx, err := sch.Indexes().AddIndexByColNames("t_partial", []string{"p"}, nil, schema.IndexProperties{
		IsUnique:      true,
		IsUserDefined: true,
		Predicate:     "t.slot IS NULL",
	})
	require.NoError(t, err)

	return partialIndexTestTable{
		sch: sch,
		idx: idx,
		vrw: ddb.ValueReadWriter(),
		ns:  ddb.NodeStore(),
		bp:  pool.NewBuffPool(),
	}
}

// row returns the key and value tuples for the row (|id|, |p|, |slot|). A nil |slot| is NULL.
func (tt partialIndexTestTable) row(t *testing.T, id, p int32, slot *int32) (val.Tuple, val.Tuple) {
	ctx := context.Background()
	kb := val.NewTupleBuilder(tt.sch.GetKeyDescriptor(tt.ns), tt.ns)
	kb.PutInt32(0, id)
	key, err := kb.Build(ctx, tt.bp)
	require.NoError(t, err)

	vb := val.NewTupleBuilder(tt.sch.GetValueDescriptor(tt.ns), tt.ns)
	vb.PutInt32(0, p)
	if slot != nil {
		vb.PutInt32(1, *slot)
	}
	value, err := vb.Build(ctx, tt.bp)
	require.NoError(t, err)
	return key, value
}

// emptyIndex returns an empty map for the partial index.
func (tt partialIndexTestTable) emptyIndex(t *testing.T) prolly.Map {
	idx, err := durable.NewEmptyIndexFromTableSchema(context.Background(), tt.vrw, tt.ns, tt.idx, tt.sch)
	require.NoError(t, err)
	m, err := durable.ProllyMapFromIndex(idx)
	require.NoError(t, err)
	return m
}

// indexedIds returns the primary keys of the rows in |m|, a map of the partial index, in index order.
func indexedIds(t *testing.T, m prolly.Map) []int32 {
	iter, err := m.IterAll(context.Background())
	require.NoError(t, err)

	var ids []int32
	for {
		k, _, err := iter.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		id, ok := m.KeyDesc().GetInt32(1, k)
		require.True(t, ok)
		ids = append(ids, id)
	}
	return ids
}

func slotOf(v int32) *int32 {
	return &v
}

func TestMutableSecondaryIdxPartialIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tt := newPartialIndexTestTable(t)

	mut, err := NewMutableSecondaryIdx(ctx, tt.emptyIndex(t), tt.sch, tt.sch, "t", tt.idx)
	require.NoError(t, err)

	k1, inside1 := tt.row(t, 1, 1, nil)
	_, outside1 := tt.row(t, 1, 1, slotOf(5))
	k2, outside2 := tt.row(t, 2, 1, slotOf(6))
	_, inside2 := tt.row(t, 2, 1, nil)
	k3, inside3 := tt.row(t, 3, 2, nil)

	require.NoError(t, applyEdit(ctx, mut, k1, nil, inside1))
	require.NoError(t, applyEdit(ctx, mut, k2, nil, outside2))
	require.NoError(t, applyEdit(ctx, mut, k3, nil, inside3))
	m, err := mut.Map(ctx)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 3}, indexedIds(t, m))

	// Row 1 moves out of the index, row 2 moves into it, and row 3 is deleted.
	require.NoError(t, applyEdit(ctx, mut, k1, inside1, outside1))
	require.NoError(t, applyEdit(ctx, mut, k2, outside2, inside2))
	require.NoError(t, applyEdit(ctx, mut, k3, inside3, nil))
	m, err = mut.Map(ctx)
	require.NoError(t, err)
	require.Equal(t, []int32{2}, indexedIds(t, m))

	// Deleting a row outside the index leaves the index unchanged.
	require.NoError(t, applyEdit(ctx, mut, k1, outside1, nil))
	m, err = mut.Map(ctx)
	require.NoError(t, err)
	require.Equal(t, []int32{2}, indexedIds(t, m))
}

func TestUniqIndexPartialIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tt := newPartialIndexTestTable(t)

	k1, v1 := tt.row(t, 1, 1, nil)
	clustered, err := prolly.NewMapFromTuples(ctx, tt.ns, tt.sch.GetKeyDescriptor(tt.ns), tt.sch.GetValueDescriptor(tt.ns), k1, v1)
	require.NoError(t, err)

	uniq, err := newUniqIndex(ctx, tt.sch, "t", tt.idx, clustered, tt.emptyIndex(t))
	require.NoError(t, err)
	require.NoError(t, uniq.insertRow(ctx, k1, v1))

	collisions := func(key, value val.Tuple) []int32 {
		var ids []int32
		err := uniq.findCollisions(ctx, key, value, func(k, _ val.Tuple) error {
			id, ok := tt.sch.GetKeyDescriptor(tt.ns).GetInt32(0, k)
			require.True(t, ok)
			ids = append(ids, id)
			return nil
		})
		require.NoError(t, err)
		return ids
	}

	// A row outside the index shares row 1's key but can't collide with it, and isn't added to the index.
	k2, v2 := tt.row(t, 2, 1, slotOf(5))
	require.Empty(t, collisions(k2, v2))
	require.NoError(t, uniq.insertRow(ctx, k2, v2))
	require.NoError(t, uniq.clustered.Put(ctx, k2, v2))

	// A row inside the index collides only with row 1.
	k3, v3 := tt.row(t, 3, 1, nil)
	require.Equal(t, []int32{1, 3}, collisions(k3, v3))
}

func TestBuildIndexPartialIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tt := newPartialIndexTestTable(t)

	k1, v1 := tt.row(t, 1, 1, nil)
	k2, v2 := tt.row(t, 2, 1, slotOf(5))
	k3, v3 := tt.row(t, 3, 2, slotOf(6))
	k4, v4 := tt.row(t, 4, 2, nil)
	rows, err := prolly.NewMapFromTuples(ctx, tt.ns, tt.sch.GetKeyDescriptor(tt.ns), tt.sch.GetValueDescriptor(tt.ns),
		k1, v1, k2, v2, k3, v3, k4, v4)
	require.NoError(t, err)

	for _, unique := range []bool{true, false} {
		idx, err := tt.sch.Indexes().AddIndexByColNames("t_partial_built", []string{"p"}, nil, schema.IndexProperties{
			IsUnique:      unique,
			IsUserDefined: true,
			Predicate:     "t.slot IS NULL",
		})
		require.NoError(t, err)

		// No rows inside the index collide, so no constraint violations are recorded in the nil artifacts editor.
		built, err := buildIndex(ctx, tt.vrw, tt.ns, tt.sch, idx, rows, nil, nil, "t")
		require.NoError(t, err)
		m, err := durable.ProllyMapFromIndex(built)
		require.NoError(t, err)
		require.Equal(t, []int32{1, 4}, indexedIds(t, m))

		_, err = tt.sch.Indexes().RemoveIndex("t_partial_built")
		require.NoError(t, err)
	}
}
