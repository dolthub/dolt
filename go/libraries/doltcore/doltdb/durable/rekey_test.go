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

package durable

import (
	"context"
	"errors"
	"strings"
	"testing"

	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestRowRekeyerBasicAndVirtualColumns(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	virtCol := schema.NewColumn("g", 4, types.IntKind, false)
	virtCol.Virtual = true

	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 2, types.IntKind, false),
		schema.NewColumn("v", 3, types.IntKind, false),
		virtCol,
	))
	target := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
	))

	newSch, ok, err := schema.Rekeyed(sch, target)
	require.NoError(t, err)
	require.True(t, ok)

	rk, ok := NewRowRekeyer(ns, sch, newSch)
	require.True(t, ok)

	rows := buildIntMap(t, ctx, ns, sch,
		[][]any{{1}, {2}},
		[][]any{{10, 100}, {20, 200}},
	)

	rekeyed, unpaired, hasNullKey, err := rk.RekeyMap(ctx, rows)
	require.NoError(t, err)
	require.False(t, hasNullKey)
	unpairedCnt, err := unpaired.Count()
	require.NoError(t, err)
	require.Zero(t, unpairedCnt)

	newKD, newVD := newSch.GetMapDescriptors(ns)
	got := collectIntRows(t, ctx, rekeyed, newKD, newVD)
	require.Equal(t, [][2][]any{
		{{10, 1}, {100}},
		{{20, 2}, {200}},
	}, got)
}

func TestRowRekeyerNullAndDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 2, types.IntKind, false),
		schema.NewColumn("v", 3, types.IntKind, false),
	))
	target := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{}),
	))

	newSch, ok, err := schema.Rekeyed(sch, target)
	require.NoError(t, err)
	require.True(t, ok)

	newKD, newVD := newSch.GetMapDescriptors(ns)
	require.False(t, newKD.Types[0].Nullable)

	rk, ok := NewRowRekeyer(ns, sch, newSch)
	require.True(t, ok)

	// Row 1 has unique b=10. Rows 2 and 3 collide on b=20. Row 4 has NULL b.
	rows := buildIntMap(t, ctx, ns, sch,
		[][]any{{1}, {2}, {3}, {4}},
		[][]any{{10, 100}, {20, 200}, {20, 300}, {nil, 400}},
	)

	rekeyed, unpaired, hasNullKey, err := rk.RekeyMap(ctx, rows)
	require.NoError(t, err)
	require.True(t, hasNullKey)

	gotRekeyed := collectIntRows(t, ctx, rekeyed, newKD, newVD)
	require.Equal(t, [][2][]any{
		{{10}, {1, 100}},
	}, gotRekeyed)

	oldKD, oldVD := sch.GetMapDescriptors(ns)
	gotUnpaired := collectIntRows(t, ctx, unpaired, oldKD, oldVD)
	require.Equal(t, [][2][]any{
		{{2}, {20, 200}},
		{{3}, {20, 300}},
		{{4}, {nil, 400}},
	}, gotUnpaired)
}

func TestNewRowRekeyerRejectsMissingOrIncompatibleColumn(t *testing.T) {
	ns := tree.NewTestNodeStore()

	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 2, types.IntKind, false),
	))

	missingColSch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("c", 99, types.IntKind, false),
	))
	_, ok := NewRowRekeyer(ns, sch, missingColSch)
	require.False(t, ok)

	diffEncSch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("b", 2, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("a", 1, types.IntKind, false),
	))
	_, ok = NewRowRekeyer(ns, sch, diffEncSch)
	require.False(t, ok)

	keylessSch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.UintKind, false),
	))
	keyedUintSch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.UintKind, true, schema.NotNullConstraint{}),
	))
	_, ok = NewRowRekeyer(ns, keylessSch, keyedUintSch)
	require.False(t, ok)
	_, ok = NewRowRekeyer(ns, keyedUintSch, keylessSch)
	require.False(t, ok)
}

func TestRowRekeyerRecyclesBuildersOnError(t *testing.T) {
	prev := typeinfo.UseAdaptiveEncoding
	typeinfo.UseAdaptiveEncoding = true
	t.Cleanup(func() { typeinfo.UseAdaptiveEncoding = prev })

	ctx := context.Background()
	baseNS := tree.NewTestNodeStore()
	ns := &failingWriteNodeStore{NodeStore: baseNS}

	textTI, err := typeinfo.FromSqlType(gmstypes.Text)
	require.NoError(t, err)
	textCol, err := schema.NewColumnWithTypeInfo("txt", 2, textTI, false, "", false, "")
	require.NoError(t, err)

	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
		textCol,
	))

	rk, ok := NewRowRekeyer(ns, sch, sch)
	require.True(t, ok)
	rk.vb = rk.vb.WithMaxRowSize(100)

	oldKD, oldVD := sch.GetMapDescriptors(ns)
	kb := val.NewTupleBuilder(oldKD, ns)
	vb := val.NewTupleBuilder(oldVD, ns).WithMaxRowSize(10000)

	// Row 1 has an inline TEXT field (> 100B) that forces out-of-band WriteBytes
	// when rebuilt under rk.vb's smaller row size limit.
	kb.PutInt64(0, 1)
	require.NoError(t, vb.PutAdaptiveStringFromInline(ctx, 0, strings.Repeat("x", 200)))
	k1, err := kb.Build(ctx, ns.Pool())
	require.NoError(t, err)
	v1, err := vb.Build(ctx, ns.Pool())
	require.NoError(t, err)

	// Row 2 has NULL in the TEXT field.
	kb.PutInt64(0, 2)
	k2, err := kb.Build(ctx, ns.Pool())
	require.NoError(t, err)
	v2, err := vb.Build(ctx, ns.Pool())
	require.NoError(t, err)

	ns.failWrite = true
	_, _, err = rk.Rekey(ctx, k1, v1)
	require.Error(t, err)

	ns.failWrite = false
	_, nv2, err := rk.Rekey(ctx, k2, v2)
	require.NoError(t, err)
	require.True(t, rk.newVD.IsNull(0, nv2))
}

func TestRowRekeyerUniqueMap(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 2, types.IntKind, false),
		schema.NewColumn("v", 3, types.IntKind, false),
	))
	target := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{}),
	))

	newSch, ok, err := schema.Rekeyed(sch, target)
	require.NoError(t, err)
	require.True(t, ok)

	rk, ok := NewRowRekeyer(ns, sch, newSch)
	require.True(t, ok)

	uniqueRows := buildIntMap(t, ctx, ns, sch,
		[][]any{{1}, {2}},
		[][]any{{10, 100}, {20, 200}},
	)
	rekeyed, err := rk.RekeyUniqueMap(ctx, uniqueRows)
	require.NoError(t, err)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	require.Equal(t, [][2][]any{
		{{10}, {1, 100}},
		{{20}, {2, 200}},
	}, collectIntRows(t, ctx, rekeyed, newKD, newVD))

	nullRows := buildIntMap(t, ctx, ns, sch,
		[][]any{{1}, {2}},
		[][]any{{10, 100}, {nil, 200}},
	)
	_, err = rk.RekeyUniqueMap(ctx, nullRows)
	require.ErrorIs(t, err, ErrRekeyNullKey)

	dupRows := buildIntMap(t, ctx, ns, sch,
		[][]any{{1}, {2}},
		[][]any{{10, 100}, {10, 200}},
	)
	_, err = rk.RekeyUniqueMap(ctx, dupRows)
	require.ErrorIs(t, err, ErrRekeyDuplicateKey)
}

type failingWriteNodeStore struct {
	tree.NodeStore
	failWrite bool
}

// WriteBytes returns an injected error when failWrite is true.
func (f *failingWriteNodeStore) WriteBytes(ctx context.Context, bs []byte) (hash.Hash, error) {
	if f.failWrite {
		return hash.Hash{}, errors.New("injected write failure")
	}
	return f.NodeStore.WriteBytes(ctx, bs)
}

// buildIntMap constructs a [prolly.Map] from integer key and value slices.
func buildIntMap(t *testing.T, ctx context.Context, ns tree.NodeStore, sch schema.Schema, keys [][]any, vals [][]any) prolly.Map {
	t.Helper()
	kd, vd := sch.GetMapDescriptors(ns)
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	require.NoError(t, err)
	mut := m.Mutate()
	kb := val.NewTupleBuilder(kd, ns)
	vb := val.NewTupleBuilder(vd, ns)
	for i := range keys {
		for j, v := range keys[i] {
			if v != nil {
				kb.PutInt64(j, int64(v.(int)))
			}
		}
		for j, v := range vals[i] {
			if v != nil {
				vb.PutInt64(j, int64(v.(int)))
			}
		}
		k, err := kb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		v, err := vb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		require.NoError(t, mut.Put(ctx, k, v))
	}
	res, err := mut.Map(ctx)
	require.NoError(t, err)
	return res
}

// collectIntRows reads all rows of |m| into integer key/value pairs.
func collectIntRows(t *testing.T, ctx context.Context, m prolly.Map, kd, vd *val.TupleDesc) [][2][]any {
	t.Helper()
	var out [][2][]any
	err := IterRows(ctx, m, func(k, v val.Tuple) error {
		ks := make([]any, kd.Count())
		for i := 0; i < kd.Count(); i++ {
			if val64, ok := kd.GetInt64(i, k); ok {
				ks[i] = int(val64)
			}
		}
		vs := make([]any, vd.Count())
		for i := 0; i < vd.Count(); i++ {
			if val64, ok := vd.GetInt64(i, v); ok {
				vs[i] = int(val64)
			}
		}
		out = append(out, [2][]any{ks, vs})
		return nil
	})
	require.NoError(t, err)
	return out
}
