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

package schema

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

func TestMatchingColumn(t *testing.T) {
	sch := MustSchemaFromCols(NewColCollection(
		NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("b", 2, types.IntKind, false),
	))

	t.Run("tag_match_across_rename", func(t *testing.T) {
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("a_renamed", 1, types.IntKind, true, NotNullConstraint{}),
		))
		col, ok := MatchingColumn(sch, target, target.GetPKCols().GetByIndex(0))
		require.True(t, ok)
		require.Equal(t, uint64(1), col.Tag)
		require.Equal(t, "a", col.Name)
	})

	t.Run("case_insensitive_name_match_after_recreate", func(t *testing.T) {
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("B", 20, types.IntKind, true, NotNullConstraint{}),
		))
		col, ok := MatchingColumn(sch, target, target.GetPKCols().GetByIndex(0))
		require.True(t, ok)
		require.Equal(t, uint64(2), col.Tag)
		require.Equal(t, "b", col.Name)
	})

	t.Run("rejects_name_match_when_source_tag_exists_on_target", func(t *testing.T) {
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 20, types.IntKind, true, NotNullConstraint{}),
			NewColumn("other", 2, types.IntKind, false),
		))
		_, ok := MatchingColumn(sch, target, target.GetPKCols().GetByIndex(0))
		require.False(t, ok)
	})

	t.Run("missing_column", func(t *testing.T) {
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("c", 30, types.IntKind, true, NotNullConstraint{}),
		))
		_, ok := MatchingColumn(sch, target, target.GetPKCols().GetByIndex(0))
		require.False(t, ok)
	})
}

func TestRekeyed(t *testing.T) {
	mustColWithType := func(name string, tag uint64, typ sql.Type, partOfPK bool, constraints ...ColConstraint) Column {
		ti, err := typeinfo.FromSqlType(typ)
		require.NoError(t, err)
		col, err := NewColumnWithTypeInfo(name, tag, ti, partOfPK, "", false, "", constraints...)
		require.NoError(t, err)
		return col
	}

	t.Run("promote_and_demote_by_tag", func(t *testing.T) {
		sch := MustSchemaFromCols(NewColCollection(
			NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
			NewColumn("b", 2, types.IntKind, false),
			NewColumn("v", 3, types.IntKind, false),
		))
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 2, types.IntKind, true, NotNullConstraint{}),
			NewColumn("a", 1, types.IntKind, false),
		))
		rekeyed, ok, err := Rekeyed(sch, target)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []uint64{2}, rekeyed.GetPKCols().Tags)
		bCol, ok := rekeyed.GetPKCols().GetByTag(2)
		require.True(t, ok)
		require.False(t, bCol.IsNullable())
		require.Contains(t, bCol.Constraints, NotNullConstraint{})
		kd, _ := rekeyed.GetMapDescriptors(nil)
		require.False(t, kd.Types[0].Nullable)
		aCol, ok := rekeyed.GetNonPKCols().GetByTag(1)
		require.True(t, ok)
		require.False(t, aCol.IsPartOfPK)
	})

	t.Run("name_match_after_recreate", func(t *testing.T) {
		sch := MustSchemaFromCols(NewColCollection(
			NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
			NewColumn("b", 2, types.IntKind, false),
		))
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 20, types.IntKind, true, NotNullConstraint{}),
		))
		rekeyed, ok, err := Rekeyed(sch, target)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []uint64{2}, rekeyed.GetPKCols().Tags)
	})

	t.Run("rejects_tag_collision", func(t *testing.T) {
		sch := MustSchemaFromCols(NewColCollection(
			NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
			NewColumn("b", 2, types.IntKind, false),
		))
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 20, types.IntKind, true, NotNullConstraint{}),
			NewColumn("c", 2, types.IntKind, false),
		))
		_, ok, err := Rekeyed(sch, target)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("allows_non_pk_virtual_column_rejects_virtual_pk", func(t *testing.T) {
		virtCol := NewColumn("g", 3, types.IntKind, false)
		virtCol.Virtual = true

		sch := MustSchemaFromCols(NewColCollection(
			NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
			NewColumn("b", 2, types.IntKind, false),
			virtCol,
		))
		targetValid := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 2, types.IntKind, true, NotNullConstraint{}),
		))
		rekeyed, ok, err := Rekeyed(sch, targetValid)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []uint64{2}, rekeyed.GetPKCols().Tags)

		targetVirtPK := MustSchemaFromCols(NewColCollection(
			NewColumn("g", 3, types.IntKind, true, NotNullConstraint{}),
		))
		_, ok, err = Rekeyed(sch, targetVirtPK)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("rejects_fulltext_spatial_vector_indexes", func(t *testing.T) {
		makeBase := func() Schema {
			return MustSchemaFromCols(NewColCollection(
				NewColumn("a", 1, types.IntKind, true, NotNullConstraint{}),
				NewColumn("b", 2, types.IntKind, false),
				NewColumn("s", 3, types.StringKind, false),
			))
		}
		target := MustSchemaFromCols(NewColCollection(
			NewColumn("b", 2, types.IntKind, true, NotNullConstraint{}),
		))

		for _, tc := range []struct {
			name string
			add  func(Schema) error
		}{
			{
				name: "fulltext",
				add: func(s Schema) error {
					_, err := s.Indexes().AddIndexByColNames("ft", []string{"s"}, nil, IndexProperties{IsFullText: true})
					return err
				},
			},
			{
				name: "spatial",
				add: func(s Schema) error {
					_, err := s.Indexes().AddIndexByColNames("sp", []string{"s"}, nil, IndexProperties{IsSpatial: true})
					return err
				},
			},
			{
				name: "vector",
				add: func(s Schema) error {
					_, err := s.Indexes().AddIndexByColNames("vec", []string{"s"}, nil, IndexProperties{IsVector: true})
					return err
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := makeBase()
				require.NoError(t, tc.add(s))
				_, ok, err := Rekeyed(s, target)
				require.NoError(t, err)
				require.False(t, ok)
			})
		}
	})

	t.Run("type_compatibility", func(t *testing.T) {
		vc20 := gmstypes.MustCreateString(sqltypes.VarChar, 20, sql.Collation_utf8mb4_0900_ai_ci)
		vc30 := gmstypes.MustCreateString(sqltypes.VarChar, 30, sql.Collation_utf8mb4_0900_ai_ci)
		vc30Bin := gmstypes.MustCreateString(sqltypes.VarChar, 30, sql.Collation_utf8mb4_bin)
		char5 := gmstypes.MustCreateString(sqltypes.Char, 5, sql.Collation_utf8mb4_0900_ai_ci)
		char10 := gmstypes.MustCreateString(sqltypes.Char, 10, sql.Collation_utf8mb4_0900_ai_ci)
		enumAB := gmstypes.MustCreateEnumType([]string{"a", "b"}, sql.Collation_utf8mb4_0900_ai_ci)
		enumBA := gmstypes.MustCreateEnumType([]string{"b", "a"}, sql.Collation_utf8mb4_0900_ai_ci)

		makeSch := func(typ sql.Type) Schema {
			return MustSchemaFromCols(NewColCollection(
				NewColumn("id", 1, types.IntKind, true, NotNullConstraint{}),
				mustColWithType("k", 2, typ, false, NotNullConstraint{}),
			))
		}
		makeTarget := func(typ sql.Type) Schema {
			return MustSchemaFromCols(NewColCollection(
				mustColWithType("k", 2, typ, true, NotNullConstraint{}),
			))
		}

		// Widening VARCHAR with identical collation succeeds and updates PK TypeInfo.
		rekeyed, ok, err := Rekeyed(makeSch(vc20), makeTarget(vc30))
		require.NoError(t, err)
		require.True(t, ok)
		kCol, ok := rekeyed.GetPKCols().GetByTag(2)
		require.True(t, ok)
		require.True(t, kCol.TypeInfo.ToSqlType().Equals(vc30))

		// Narrowing, collation change, CHAR resize, INT->BIGINT, and ENUM
		// reorder are rejected.
		for _, tc := range []struct {
			name string
			from sql.Type
			to   sql.Type
		}{
			{"varchar_narrowing", vc30, vc20},
			{"varchar_collation_change", vc20, vc30Bin},
			{"char_resize", char5, char10},
			{"int_to_bigint", gmstypes.Int32, gmstypes.Int64},
			{"enum_reorder", enumAB, enumBA},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, ok, err := Rekeyed(makeSch(tc.from), makeTarget(tc.to))
				require.NoError(t, err)
				require.False(t, ok)
			})
		}
	})
}
