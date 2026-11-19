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

package diff

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestAlignSchema(t *testing.T) {
	aPK := schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{})
	bNonPK := schema.NewColumn("b", 2, types.IntKind, false)
	bPK := schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{})
	cPK := schema.NewColumn("c", 3, types.IntKind, true, schema.NotNullConstraint{})

	fromSch := schema.MustSchemaFromCols(schema.NewColCollection(aPK, bNonPK))

	t.Run("nil keyless and empty", func(t *testing.T) {
		keyless := schema.MustSchemaFromCols(schema.NewColCollection(
			schema.NewColumn("a", 1, types.IntKind, false),
			bNonPK,
		))
		for _, tc := range []struct {
			from, to schema.Schema
		}{
			{nil, fromSch},
			{fromSch, nil},
			{keyless, fromSch},
			{fromSch, keyless},
			{schema.EmptySchema, fromSch},
			{fromSch, schema.EmptySchema},
		} {
			sch, aligned, err := AlignSchema(tc.from, tc.to)
			require.NoError(t, err)
			require.False(t, aligned)
			require.Nil(t, sch)
		}
	})

	t.Run("shared columns use to key order", func(t *testing.T) {
		toSch := schema.MustSchemaFromCols(schema.NewColCollection(bPK, aPK, cPK))
		sch, aligned, err := AlignSchema(fromSch, toSch)
		require.NoError(t, err)
		require.True(t, aligned)
		require.Equal(t, []string{"b", "a"}, sch.GetPKCols().GetColumnNames())
	})

	t.Run("no common key returns unaligned without error", func(t *testing.T) {
		toSch := schema.MustSchemaFromCols(schema.NewColCollection(cPK))
		sch, aligned, err := AlignSchema(fromSch, toSch)
		require.NoError(t, err)
		require.False(t, aligned)
		require.Nil(t, sch)
	})

	t.Run("incompatible key encoding returns alignment error", func(t *testing.T) {
		int32Col := mustColWithType(t, "a", 1, gmstypes.Int32, true)
		int64Col := mustColWithType(t, "a", 1, gmstypes.Int64, true)
		s1 := schema.MustSchemaFromCols(schema.NewColCollection(int32Col))
		s2 := schema.MustSchemaFromCols(schema.NewColCollection(int64Col))
		sch, aligned, err := AlignSchema(s1, s2)
		require.Error(t, err)
		require.True(t, IsPrimaryKeyAlignmentError(err))
		require.False(t, aligned)
		require.Nil(t, sch)
	})

	t.Run("name collision with renamed column returns alignment error", func(t *testing.T) {
		bOldRenamed := schema.NewColumn("b_old", 2, types.IntKind, false)
		bNew := schema.NewColumn("b", 99, types.IntKind, true, schema.NotNullConstraint{})
		toSch := schema.MustSchemaFromCols(schema.NewColCollection(bOldRenamed, bNew))
		sch, aligned, err := AlignSchema(fromSch, toSch)
		require.Error(t, err)
		require.True(t, IsPrimaryKeyAlignmentError(err))
		require.False(t, aligned)
		require.Nil(t, sch)
	})
}

func TestIsSameStoredKey(t *testing.T) {
	vc20 := mustColWithType(t, "code", 1, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 20), true)
	vc30 := mustColWithType(t, "code", 1, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 30), true)
	s20 := schema.MustSchemaFromCols(schema.NewColCollection(vc20))
	s30 := schema.MustSchemaFromCols(schema.NewColCollection(vc30))
	require.True(t, IsSameStoredKey(s20, s30))

	otherCol := mustColWithType(t, "other", 2, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 20), true)
	sOther := schema.MustSchemaFromCols(schema.NewColCollection(otherCol))
	require.False(t, IsSameStoredKey(s20, sOther))
}

func TestAlignPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	aPK := schema.NewColumn("a", 1, types.IntKind, true, schema.NotNullConstraint{})
	bNonPK := schema.NewColumn("b", 2, types.IntKind, false)
	aNonPK := schema.NewColumn("a", 1, types.IntKind, false)
	bPK := schema.NewColumn("b", 2, types.IntKind, true, schema.NotNullConstraint{})
	cPK := schema.NewColumn("c", 3, types.IntKind, true, schema.NotNullConstraint{})

	fromSch := schema.MustSchemaFromCols(schema.NewColCollection(aPK, bNonPK))
	toSch := schema.MustSchemaFromCols(schema.NewColCollection(aNonPK, bPK))

	t.Run("rekeys from side and leaves duplicate and NULL keys unpaired", func(t *testing.T) {
		fromMap := buildTestIntMap(t, ctx, ns, fromSch,
			[][]any{{1}, {2}, {3}, {4}},
			[][]any{{10}, {10}, {nil}, {20}},
		)
		toMap := buildTestIntMap(t, ctx, ns, toSch,
			[][]any{{20}},
			[][]any{{4}},
		)
		fromAligned, toAligned, err := AlignPrimaryKeys(ctx,
			KeyedRows{Rows: fromMap, Sch: fromSch},
			KeyedRows{Rows: toMap, Sch: toSch},
		)
		require.NoError(t, err)
		require.False(t, fromAligned.NoCommonKey)
		require.Equal(t, 3, fromAligned.UnpairedCount)
		require.Equal(t, 0, toAligned.UnpairedCount)

		cnt, err := fromAligned.Rows.Count()
		require.NoError(t, err)
		require.Equal(t, 1, cnt)
	})

	t.Run("no common key unpairs every row", func(t *testing.T) {
		fromMap := buildTestIntMap(t, ctx, ns, fromSch, [][]any{{1}, {2}}, [][]any{{10}, {20}})
		otherSch := schema.MustSchemaFromCols(schema.NewColCollection(cPK))
		toMap := buildTestIntMap(t, ctx, ns, otherSch, [][]any{{100}}, [][]any{{}})

		fromAligned, toAligned, err := AlignPrimaryKeys(ctx,
			KeyedRows{Rows: fromMap, Sch: fromSch},
			KeyedRows{Rows: toMap, Sch: otherSch},
		)
		require.NoError(t, err)
		require.True(t, fromAligned.NoCommonKey)
		require.True(t, toAligned.NoCommonKey)
		require.Equal(t, 2, fromAligned.UnpairedCount)
		require.Equal(t, 1, toAligned.UnpairedCount)
		cnt, err := fromAligned.Rows.Count()
		require.NoError(t, err)
		require.Equal(t, 0, cnt)
	})

	t.Run("keyed and keyless unpair every row", func(t *testing.T) {
		fromMap := buildTestIntMap(t, ctx, ns, fromSch, [][]any{{1}}, [][]any{{10}})
		keylessSch := schema.MustSchemaFromCols(schema.NewColCollection(aNonPK, bNonPK))
		kd, vd := keylessSch.GetMapDescriptors(ns)
		toMap, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
		require.NoError(t, err)

		fromAligned, toAligned, err := AlignPrimaryKeys(ctx,
			KeyedRows{Rows: fromMap, Sch: fromSch},
			KeyedRows{Rows: toMap, Sch: keylessSch},
		)
		require.NoError(t, err)
		require.True(t, fromAligned.NoCommonKey)
		require.True(t, toAligned.NoCommonKey)
		require.Equal(t, 1, fromAligned.UnpairedCount)
		require.Equal(t, 0, toAligned.UnpairedCount)
	})
}

// mustColWithType builds a schema.Column with the given SQL type for tests.
func mustColWithType(t *testing.T, name string, tag uint64, sqlType sql.Type, isPK bool) schema.Column {
	t.Helper()
	ti, err := typeinfo.FromSqlType(sqlType)
	require.NoError(t, err)
	var constraints []schema.ColConstraint
	if isPK {
		constraints = []schema.ColConstraint{schema.NotNullConstraint{}}
	}
	col, err := schema.NewColumnWithTypeInfo(name, tag, ti, isPK, "", false, "", constraints...)
	require.NoError(t, err)
	return col
}

// buildTestIntMap builds an in-memory prolly.Map with integer keys and values
// for primary key alignment tests.
func buildTestIntMap(t *testing.T, ctx context.Context, ns tree.NodeStore, sch schema.Schema, keys [][]any, vals [][]any) prolly.Map {
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
