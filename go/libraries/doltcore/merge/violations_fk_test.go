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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/val"
)

// fkRecordingReceiver records the foreign keys that RegisterForeignKeyViolations checks.
type fkRecordingReceiver struct {
	started []string
}

var _ FKViolationReceiver = (*fkRecordingReceiver)(nil)

func (r *fkRecordingReceiver) StartFK(_ *sql.Context, fk doltdb.ForeignKey) error {
	r.started = append(r.started, fk.Name)
	return nil
}

func (r *fkRecordingReceiver) EndCurrFK(context.Context) error {
	return nil
}

func (r *fkRecordingReceiver) ProllyFKViolationFound(context.Context, val.Tuple, val.Tuple) error {
	return nil
}

// fkCountingResolver counts the tables RegisterForeignKeyViolations loads.
type fkCountingResolver struct {
	doltdb.SimpleTableResolver
	resolved int
}

func (r *fkCountingResolver) ResolveTableInsensitive(ctx *sql.Context, root doltdb.RootValue, tblName doltdb.TableName) (doltdb.TableName, *doltdb.Table, bool, error) {
	r.resolved++
	return r.SimpleTableResolver.ResolveTableInsensitive(ctx, root, tblName)
}

// TestRegisterForeignKeyViolationsSkipsUnchangedTables verifies that RegisterForeignKeyViolations only checks the
// foreign keys whose child or parent table may differ between the two roots, and loads no table for the others.
func TestRegisterForeignKeyViolationsSkipsUnchangedTables(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ddb := dtestutils.CreateTestEnv().DoltDB(ctx)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	nextTag := uint64(1)
	// newTable returns an empty table with a primary key column and |extraCols| more columns, all with fresh tags, and
	// the tag of the first column after the primary key.
	newTable := func(extraCols int) (*doltdb.Table, uint64) {
		cols := []schema.Column{mustCol(t, "pk", nextTag, typeinfo.Int32Type, true)}
		nextTag++
		firstExtra := nextTag
		for i := 0; i < extraCols; i++ {
			cols = append(cols, mustCol(t, "c"+string(rune('0'+i)), nextTag, typeinfo.Int32Type, false))
			nextTag++
		}
		tbl, err := doltdb.NewEmptyTable(ctx, vrw, ns, schema.MustSchemaFromCols(schema.NewColCollection(cols...)))
		require.NoError(t, err)
		return tbl, firstExtra
	}
	put := func(root doltdb.RootValue, name string, tbl *doltdb.Table) doltdb.RootValue {
		root, err := root.PutTable(ctx, doltdb.TableName{Name: name}, tbl)
		require.NoError(t, err)
		return root
	}

	base, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)

	// Two parents, each referenced by one child through the parent's primary key, and a table without foreign keys.
	var fks []doltdb.ForeignKey
	for _, pair := range []struct{ fk, child, parent string }{
		{"fk1", "child1", "parent1"},
		{"fk2", "child2", "parent2"},
	} {
		parent, _ := newTable(0)
		base = put(base, pair.parent, parent)
		parentSch, err := parent.GetSchema(ctx)
		require.NoError(t, err)
		child, childFkTag := newTable(1)
		base = put(base, pair.child, child)
		fks = append(fks, doltdb.ForeignKey{
			Name:                   pair.fk,
			TableName:              doltdb.TableName{Name: pair.child},
			TableColumns:           []uint64{childFkTag},
			ReferencedTableName:    doltdb.TableName{Name: pair.parent},
			ReferencedTableColumns: parentSch.GetPKCols().Tags,
		})
	}
	unrelated, _ := newTable(1)
	base = put(base, "unrelated", unrelated)
	fkc, err := doltdb.NewForeignKeyCollection(fks...)
	require.NoError(t, err)
	base, err = base.PutForeignKeyCollection(ctx, fkc)
	require.NoError(t, err)

	// changed returns a table that differs from every table in |base|.
	changed := func() *doltdb.Table {
		tbl, _ := newTable(2)
		return tbl
	}

	tests := []struct {
		name    string
		newRoot func() doltdb.RootValue
		checked []string
	}{
		{
			name:    "identical roots",
			newRoot: func() doltdb.RootValue { return base },
			checked: nil,
		},
		{
			name:    "table without foreign keys changed",
			newRoot: func() doltdb.RootValue { return put(base, "unrelated", changed()) },
			checked: nil,
		},
		{
			name:    "table added",
			newRoot: func() doltdb.RootValue { return put(base, "added", changed()) },
			checked: nil,
		},
		{
			name:    "child changed",
			newRoot: func() doltdb.RootValue { return put(base, "child1", changed()) },
			checked: []string{"fk1"},
		},
		{
			name:    "parent changed",
			newRoot: func() doltdb.RootValue { return put(base, "parent2", changed()) },
			checked: []string{"fk2"},
		},
		{
			// A resolver matches names case-insensitively, so a changed table whose name differs only in case could
			// be the table a foreign key resolves to.
			name:    "table with a case-insensitively equal name added",
			newRoot: func() doltdb.RootValue { return put(base, "CHILD2", changed()) },
			checked: []string{"fk2"},
		},
		{
			// dolt_nonlocal_tables can redirect any table name to another branch.
			name:    "dolt_nonlocal_tables changed",
			newRoot: func() doltdb.RootValue { return put(base, doltdb.GetNonlocalTablesTableName(), changed()) },
			checked: []string{"fk1", "fk2"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receiver := &fkRecordingReceiver{}
			resolver := &fkCountingResolver{}
			err := RegisterForeignKeyViolations(ctx, resolver, test.newRoot(), base, nil, receiver)
			require.NoError(t, err)
			require.Equal(t, test.checked, receiver.started)
			// A checked foreign key loads its child and its parent from both roots.
			require.Equal(t, 4*len(test.checked), resolver.resolved)
		})
	}

	t.Run("foreign key on a missing table is not skipped", func(t *testing.T) {
		missing := doltdb.ForeignKey{
			Name:                   "fk_missing",
			TableName:              doltdb.TableName{Name: "missing_child"},
			TableColumns:           []uint64{1},
			ReferencedTableName:    doltdb.TableName{Name: "parent1"},
			ReferencedTableColumns: fks[0].ReferencedTableColumns,
		}
		fkc, err := doltdb.NewForeignKeyCollection(missing)
		require.NoError(t, err)
		root, err := base.PutForeignKeyCollection(ctx, fkc)
		require.NoError(t, err)

		// Neither root has the child table, so no table differs. The key must still reach the resolver and fail as
		// it did before unchanged tables were skipped.
		receiver := &fkRecordingReceiver{}
		err = RegisterForeignKeyViolations(ctx, &fkCountingResolver{}, root, root, nil, receiver)
		require.ErrorIs(t, err, doltdb.ErrTableNotFound)
		require.Equal(t, []string{"fk_missing"}, receiver.started)
	})
}
