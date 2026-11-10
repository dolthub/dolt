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

package diff_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// fkCountingRoot counts how often the foreign key collection of a root is read.
type fkCountingRoot struct {
	doltdb.RootValue
	fkCollectionReads int
}

func (r *fkCountingRoot) GetForeignKeyCollection(ctx context.Context) (*doltdb.ForeignKeyCollection, error) {
	r.fkCollectionReads++
	return r.RootValue.GetForeignKeyCollection(ctx)
}

// TestGetTableDeltasReadsForeignKeysOncePerRoot verifies that GetTableDeltas reads the foreign key collection of each
// root once, however many tables the roots have, and still attaches to every delta the foreign keys its table declares.
func TestGetTableDeltasReadsForeignKeysOncePerRoot(t *testing.T) {
	ctx := context.Background()
	ddb := dtestutils.CreateTestEnv().DoltDB(ctx)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	const numChildren = 8
	const parentTag = uint64(1000)
	parentName := doltdb.TableName{Name: "parent"}

	newTable := func(colName string, tag uint64) *doltdb.Table {
		sch := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn(colName, tag, types.IntKind, true)))
		tbl, err := doltdb.NewEmptyTable(ctx, vrw, ns, sch)
		require.NoError(t, err)
		return tbl
	}
	childName := func(i int) doltdb.TableName {
		return doltdb.TableName{Name: fmt.Sprintf("child%d", i)}
	}
	childFk := func(i int) doltdb.ForeignKey {
		declaredOn := childName(i)
		if i == 0 {
			// Foreign keys are matched to their table case-insensitively.
			declaredOn.Name = "CHILD0"
		}
		return doltdb.ForeignKey{
			Name:                   fmt.Sprintf("fk%d", i),
			TableName:              declaredOn,
			TableColumns:           []uint64{uint64(i)},
			ReferencedTableName:    parentName,
			ReferencedTableColumns: []uint64{parentTag},
		}
	}

	// |from| has the parent and every child but the last one. |to| adds the last child and its foreign key.
	from, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)
	from, err = from.PutTable(ctx, parentName, newTable("pk", parentTag))
	require.NoError(t, err)
	var fks []doltdb.ForeignKey
	for i := 0; i < numChildren-1; i++ {
		from, err = from.PutTable(ctx, childName(i), newTable(fmt.Sprintf("c%d", i), uint64(i)))
		require.NoError(t, err)
		fks = append(fks, childFk(i))
	}
	fromFkc, err := doltdb.NewForeignKeyCollection(fks...)
	require.NoError(t, err)
	from, err = from.PutForeignKeyCollection(ctx, fromFkc)
	require.NoError(t, err)

	last := numChildren - 1
	to, err := from.PutTable(ctx, childName(last), newTable(fmt.Sprintf("c%d", last), uint64(last)))
	require.NoError(t, err)
	toFkc, err := doltdb.NewForeignKeyCollection(append(fks, childFk(last))...)
	require.NoError(t, err)
	to, err = to.PutForeignKeyCollection(ctx, toFkc)
	require.NoError(t, err)

	countedFrom := &fkCountingRoot{RootValue: from}
	countedTo := &fkCountingRoot{RootValue: to}
	deltas, err := diff.GetTableDeltas(ctx, countedFrom, countedTo)
	require.NoError(t, err)

	require.Equal(t, 1, countedFrom.fkCollectionReads, "the foreign key collection of the from root must be read once")
	require.Equal(t, 1, countedTo.fkCollectionReads, "the foreign key collection of the to root must be read once")

	// The only difference between the roots is the added child table, which declares one foreign key.
	require.Len(t, deltas, 1)
	require.True(t, deltas[0].IsAdd())
	require.Equal(t, childName(last), deltas[0].ToName)
	require.Equal(t, []doltdb.ForeignKey{childFk(last)}, deltas[0].ToFks)
	require.Contains(t, deltas[0].ToFksParentSch, parentName)

	// Reversed, the same table is a drop and its foreign key is on the from side.
	deltas, err = diff.GetTableDeltas(ctx, to, from)
	require.NoError(t, err)
	require.Len(t, deltas, 1)
	require.True(t, deltas[0].IsDrop())
	require.Equal(t, []doltdb.ForeignKey{childFk(last)}, deltas[0].FromFks)

	// Against an empty root every table is an add. Each child must get its own key, including the child whose foreign
	// key spells the table name in another case.
	empty, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)
	deltas, err = diff.GetTableDeltas(ctx, empty, from)
	require.NoError(t, err)
	require.Len(t, deltas, numChildren)
	for _, delta := range deltas {
		if delta.ToName == parentName {
			require.Empty(t, delta.ToFks)
			continue
		}
		require.Len(t, delta.ToFks, 1, "table %s", delta.ToName)
		require.True(t, delta.ToFks[0].TableName.EqualFold(delta.ToName))
	}
}
