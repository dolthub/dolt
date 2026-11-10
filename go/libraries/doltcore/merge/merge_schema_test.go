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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
)

// pruneCountingResolver counts how often each table is resolved.
type pruneCountingResolver struct {
	doltdb.SimpleTableResolver
	resolved map[doltdb.TableName]int
}

func (r *pruneCountingResolver) ResolveTable(ctx *sql.Context, root doltdb.RootValue, tblName doltdb.TableName) (*doltdb.Table, bool, error) {
	r.resolved[tblName]++
	return r.SimpleTableResolver.ResolveTable(ctx, root, tblName)
}

// TestPruneInvalidForeignKeysResolvesEachTableOnce verifies that pruneInvalidForeignKeys keeps the foreign keys whose
// tables and columns exist, drops the others, and resolves every table once however many foreign keys use it.
func TestPruneInvalidForeignKeysResolvesEachTableOnce(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ddb := dtestutils.CreateTestEnv().DoltDB(ctx)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	const (
		parentPkTag  uint64 = 1
		childPkTag   uint64 = 2
		childColTag  uint64 = 3
		missingTag   uint64 = 99
		numValidKeys        = 5
	)
	parentName := doltdb.TableName{Name: "parent"}
	childName := doltdb.TableName{Name: "child"}
	missingParentName := doltdb.TableName{Name: "missing_parent"}
	missingChildName := doltdb.TableName{Name: "missing_child"}

	parentCol, err := schema.NewColumnWithTypeInfo("pk", parentPkTag, typeinfo.Int32Type, true, "", false, "")
	require.NoError(t, err)
	childPk, err := schema.NewColumnWithTypeInfo("pk", childPkTag, typeinfo.Int32Type, true, "", false, "")
	require.NoError(t, err)
	childCol, err := schema.NewColumnWithTypeInfo("parent_id", childColTag, typeinfo.Int32Type, false, "", false, "")
	require.NoError(t, err)

	root, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)
	parent, err := doltdb.NewEmptyTable(ctx, vrw, ns, schema.MustSchemaFromCols(schema.NewColCollection(parentCol)))
	require.NoError(t, err)
	root, err = root.PutTable(ctx, parentName, parent)
	require.NoError(t, err)
	child, err := doltdb.NewEmptyTable(ctx, vrw, ns, schema.MustSchemaFromCols(schema.NewColCollection(childPk, childCol)))
	require.NoError(t, err)
	root, err = root.PutTable(ctx, childName, child)
	require.NoError(t, err)

	newFk := func(name string, table doltdb.TableName, tag uint64, refTable doltdb.TableName) doltdb.ForeignKey {
		return doltdb.ForeignKey{
			Name:                   name,
			TableName:              table,
			TableColumns:           []uint64{tag},
			ReferencedTableName:    refTable,
			ReferencedTableColumns: []uint64{parentPkTag},
		}
	}
	var expected []string
	fkc, err := doltdb.NewForeignKeyCollection()
	require.NoError(t, err)
	for i := 0; i < numValidKeys; i++ {
		name := fmt.Sprintf("fk_valid_%d", i)
		expected = append(expected, name)
		require.NoError(t, fkc.AddKeys(newFk(name, childName, childColTag, parentName)))
	}
	require.NoError(t, fkc.AddKeys(
		newFk("fk_missing_parent", childName, childColTag, missingParentName),
		newFk("fk_missing_child", missingChildName, childColTag, parentName),
		newFk("fk_missing_column", childName, missingTag, parentName),
	))

	resolver := &pruneCountingResolver{resolved: make(map[doltdb.TableName]int)}
	pruned, err := pruneInvalidForeignKeys(ctx, resolver, fkc, root)
	require.NoError(t, err)

	var kept []string
	for _, fk := range pruned.AllKeys() {
		kept = append(kept, fk.Name)
	}
	require.Equal(t, expected, kept)

	require.Equal(t, map[doltdb.TableName]int{
		parentName:        1,
		childName:         1,
		missingParentName: 1,
		missingChildName:  1,
	}, resolver.resolved)
}

// TestForeignKeysMerge verifies the merged foreign keys when neither side, one side, or both sides changed them, and that
// invalid foreign keys are pruned unless both sides have the same foreign keys.
func TestForeignKeysMerge(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ddb := dtestutils.CreateTestEnv().DoltDB(ctx)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	const parentPkTag uint64 = 1
	parentName := doltdb.TableName{Name: "parent"}
	childName := doltdb.TableName{Name: "child"}

	newCol := func(name string, tag uint64, pk bool) schema.Column {
		col, err := schema.NewColumnWithTypeInfo(name, tag, typeinfo.Int32Type, pk, "", false, "")
		require.NoError(t, err)
		return col
	}
	root, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)
	parent, err := doltdb.NewEmptyTable(ctx, vrw, ns, schema.MustSchemaFromCols(schema.NewColCollection(newCol("pk", parentPkTag, true))))
	require.NoError(t, err)
	root, err = root.PutTable(ctx, parentName, parent)
	require.NoError(t, err)
	childSch := schema.MustSchemaFromCols(schema.NewColCollection(newCol("pk", 2, true), newCol("c3", 3, false), newCol("c4", 4, false), newCol("c5", 5, false)))
	child, err := doltdb.NewEmptyTable(ctx, vrw, ns, childSch)
	require.NoError(t, err)
	root, err = root.PutTable(ctx, childName, child)
	require.NoError(t, err)

	newFk := func(name string, tag uint64, refTable doltdb.TableName) doltdb.ForeignKey {
		return doltdb.ForeignKey{
			Name:                   name,
			TableName:              childName,
			TableColumns:           []uint64{tag},
			ReferencedTableName:    refTable,
			ReferencedTableColumns: []uint64{parentPkTag},
		}
	}
	fk1 := newFk("fk1", 3, parentName)
	fk2 := newFk("fk2", 4, parentName)
	fk3 := newFk("fk3", 5, parentName)
	invalid := newFk("fk_invalid", 3, doltdb.TableName{Name: "missing_parent"})
	withKeys := func(fks ...doltdb.ForeignKey) doltdb.RootValue {
		fkc, err := doltdb.NewForeignKeyCollection(fks...)
		require.NoError(t, err)
		r, err := root.PutForeignKeyCollection(ctx, fkc)
		require.NoError(t, err)
		return r
	}
	anc := withKeys(fk1)

	tests := []struct {
		name     string
		ours     doltdb.RootValue
		theirs   doltdb.RootValue
		expected []string
	}{
		{
			name:     "only ours changed",
			ours:     withKeys(fk1, fk2, invalid),
			theirs:   anc,
			expected: []string{"fk1", "fk2"},
		},
		{
			name:     "only theirs changed",
			ours:     anc,
			theirs:   withKeys(fk1, fk2, invalid),
			expected: []string{"fk1", "fk2"},
		},
		{
			// Foreign keys that are the same on both sides are left as they are, even if they are invalid.
			name:     "neither changed",
			ours:     withKeys(fk1, invalid),
			theirs:   withKeys(fk1, invalid),
			expected: []string{"fk1", "fk_invalid"},
		},
		{
			name:     "both changed the same way",
			ours:     withKeys(fk1, fk2, invalid),
			theirs:   withKeys(fk1, fk2, invalid),
			expected: []string{"fk1", "fk2", "fk_invalid"},
		},
		{
			name:     "both changed",
			ours:     withKeys(fk1, fk2),
			theirs:   withKeys(fk3),
			expected: []string{"fk2", "fk3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			merged, conflicts, err := ForeignKeysMerge(ctx, doltdb.SimpleTableResolver{}, root, test.ours, test.theirs, anc)
			require.NoError(t, err)
			require.Empty(t, conflicts)
			var names []string
			for _, fk := range merged.AllKeys() {
				names = append(names, fk.Name)
			}
			require.Equal(t, test.expected, names)
		})
	}
}
