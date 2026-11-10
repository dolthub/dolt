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
