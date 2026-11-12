// Copyright 2023 Dolthub, Inc.
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

package index

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expranalysis"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestGetIndexKeyMapping(t *testing.T) {
	tests := []struct {
		Name    string
		AllCols []schema.Column
		IdxCols []string
		Mapping val.OrdinalMapping
		Split   int
	}{
		{
			Name: "basic",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col2"},
			Split:   1,
			Mapping: []int{1, 0},
		},
		{
			Name: "basic, pk not first",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
			},
			IdxCols: []string{"col1"},
			Split:   1,
			Mapping: []int{1, 0},
		},
		{
			Name: "compound index",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col2", "col3"},
			Split:   1,
			Mapping: []int{1, 2, 0},
		},
		{
			Name: "compound index reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col2"},
			Split:   1,
			Mapping: []int{2, 1, 0},
		},
		{
			Name: "compound index, pk not first",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col1", "col3"},
			Split:   1,
			Mapping: []int{1, 2, 0},
		},
		{
			Name: "compound index, pk not first, reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col1"},
			Split:   1,
			Mapping: []int{2, 1, 0},
		},
		{
			Name: "keyless",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col1"},
			Split:   1,
			// Mapping should skip over cardinality
			Mapping: []int{2, 0},
		},
		{
			Name: "keyless other",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col2"},
			Split:   1,
			// Mapping should skip over cardinality
			Mapping: []int{3, 0},
		},
		{
			Name: "compound keyless",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col2", "col3"},
			Split:   1,
			// Mapping should skip over cardinality
			Mapping: []int{3, 4, 0},
		},
		{
			Name: "compound keyless reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col2"},
			Split:   1,
			// Mapping should skip over cardinality
			Mapping: []int{4, 3, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			sch := schema.MustSchemaFromCols(schema.NewColCollection(tt.AllCols...))

			idxTags := make([]uint64, len(tt.IdxCols))
			for i, name := range tt.IdxCols {
				col := sch.GetAllCols().NameToCol[name]
				idxTags[i] = col.Tag
			}
			allTags := append(idxTags, sch.GetPKCols().Tags...)
			idx := schema.NewIndex("test_idx", idxTags, allTags, nil, schema.IndexProperties{})
			b, err := NewSecondaryKeyBuilder(nil, "", sch, idx, &val.TupleDesc{}, nil, nil)
			require.NoError(t, err)
			require.Equal(t, tt.Split, b.split)
			require.Equal(t, tt.Mapping, b.mapping)
		})
	}
}

func TestTupleMatchesPredicate(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	bp := pool.NewBuffPool()

	withExtra := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("id", 0, types.IntKind, true),
		schema.NewColumn("extra", 1, types.IntKind, false),
		schema.NewColumn("p", 2, types.IntKind, false),
		schema.NewColumn("slot", 3, types.IntKind, false),
	))
	withoutExtra := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("id", 0, types.IntKind, true),
		schema.NewColumn("p", 2, types.IntKind, false),
		schema.NewColumn("slot", 3, types.IntKind, false),
	))

	// buildRow returns the key and value tuples of a row of |sch| whose columns after the primary key are |vals|.
	buildRow := func(t *testing.T, sch schema.Schema, id int64, vals ...*int64) (val.Tuple, val.Tuple) {
		kb := val.NewTupleBuilder(sch.GetKeyDescriptor(ns), ns)
		kb.PutInt64(0, id)
		key, err := kb.Build(ctx, bp)
		require.NoError(t, err)

		vb := val.NewTupleBuilder(sch.GetValueDescriptor(ns), ns)
		for i, v := range vals {
			if v != nil {
				vb.PutInt64(i, *v)
			}
		}
		value, err := vb.Build(ctx, bp)
		require.NoError(t, err)
		return key, value
	}
	i := func(v int64) *int64 { return &v }

	for _, sch := range []schema.Schema{withExtra, withoutExtra} {
		idx, err := sch.Indexes().AddIndexByColNames("t_partial", []string{"p"}, nil, schema.IndexProperties{
			IsUnique:      true,
			IsUserDefined: true,
			Predicate:     "t.slot > 5",
		})
		require.NoError(t, err)

		predicate, err := expranalysis.ResolveIndexPredicate(ctx, "t", sch, idx)
		require.NoError(t, err)
		require.NotNil(t, predicate)

		hasExtra := sch.GetAllCols().Size() == 4
		rowVals := func(p, slot *int64) []*int64 {
			if hasExtra {
				return []*int64{i(0), p, slot}
			}
			return []*int64{p, slot}
		}

		tests := []struct {
			name    string
			slot    *int64
			matches bool
		}{
			{name: "satisfies predicate", slot: i(6), matches: true},
			{name: "fails predicate", slot: i(5), matches: false},
			{name: "predicate is NULL", slot: nil, matches: false},
		}
		for _, test := range tests {
			t.Run(fmt.Sprintf("%d columns, %s", sch.GetAllCols().Size(), test.name), func(t *testing.T) {
				key, value := buildRow(t, sch, 1, rowVals(i(1), test.slot)...)
				matches, err := TupleMatchesPredicate(ctx, predicate, key, value, sch, ns)
				require.NoError(t, err)
				require.Equal(t, test.matches, matches)

				matches, err = TupleMatchesPredicate(ctx, nil, key, value, sch, ns)
				require.NoError(t, err)
				require.True(t, matches)
			})
		}

		t.Run(fmt.Sprintf("%d columns, full index", sch.GetAllCols().Size()), func(t *testing.T) {
			full, err := sch.Indexes().AddIndexByColNames("t_full", []string{"p"}, nil, schema.IndexProperties{IsUserDefined: true})
			require.NoError(t, err)
			predicate, err := expranalysis.ResolveIndexPredicate(ctx, "t", sch, full)
			require.NoError(t, err)
			require.Nil(t, predicate)
		})
	}
}
