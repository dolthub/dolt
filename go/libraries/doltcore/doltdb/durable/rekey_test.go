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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestRowRekeyer(t *testing.T) {
	ctx := context.Background()

	t.Run("rekey map", func(t *testing.T) {
		ns := tree.NewTestNodeStore()

		sch := schema.MustSchemaFromCols(schema.NewColCollection(
			schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{}),
			schema.NewColumn("b", 2, types.IntKind, false),
			schema.NewColumn("v", 3, types.IntKind, false),
		))
		target := schema.MustSchemaFromCols(schema.NewColCollection(
			schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{}),
		))

		newSch, ok, err := schema.Rekey(sch, target)
		require.NoError(t, err)
		require.True(t, ok)

		newKD, newVD := newSch.GetMapDescriptors(ns)
		require.False(t, newKD.Types[0].Nullable)

		rk, ok := NewRowRekeyer(ns, sch, newSch)
		require.True(t, ok)

		// Row 1 is unique (b=10). Rows 2 and 3 collide (b=20). Row 4 has NULL b.
		rows := newIntMap(t, ctx, ns, sch,
			[][]any{{1}, {2}, {3}, {4}},
			[][]any{{10, 100}, {20, 200}, {20, 300}, {nil, 400}},
		)

		rekeyed, rejected, err := rk.RekeyMap(ctx, rows)
		require.NoError(t, err)
		require.Equal(t, [][2][]any{
			{{10}, {1, 100}},
		}, collectIntRows(t, ctx, rekeyed, newKD, newVD))

		oldKD, oldVD := sch.GetMapDescriptors(ns)
		require.Equal(t, [][2][]any{
			{{2}, {20, 200}},
			{{3}, {20, 300}},
			{{4}, {nil, 400}},
		}, collectIntRows(t, ctx, rejected, oldKD, oldVD))
	})
}

// newIntMap constructs a [prolly.Map] from integer key and value slices.
func newIntMap(t *testing.T, ctx context.Context, ns tree.NodeStore, sch schema.Schema, keys, vals [][]any) prolly.Map {
	t.Helper()
	kd, vd := sch.GetMapDescriptors(ns)
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	require.NoError(t, err)
	mut := m.Mutate()
	kb, vb := val.NewTupleBuilder(kd, ns), val.NewTupleBuilder(vd, ns)
	for i := range keys {
		putInts(kb, keys[i])
		putInts(vb, vals[i])
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
		out = append(out, [2][]any{readInts(kd, k), readInts(vd, v)})
		return nil
	})
	require.NoError(t, err)
	return out
}

// putInts writes each non-nil int element of |vals| into |b|.
func putInts(b *val.TupleBuilder, vals []any) {
	for i, v := range vals {
		if v != nil {
			b.PutInt64(i, int64(v.(int)))
		}
	}
}

// readInts decodes each int64 field of |tup| with |d|.
func readInts(d *val.TupleDesc, tup val.Tuple) []any {
	out := make([]any, d.Count())
	for i := range out {
		if v, ok := d.GetInt64(i, tup); ok {
			out[i] = int(v)
		}
	}
	return out
}
