// Copyright 2021 Dolthub, Inc.
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

package alterschema_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

const indexName string = "c1_idx"

var nomsType = types.Format_Default

func getTable(ctx context.Context, dEnv *env.DoltEnv, tableName string) (*doltdb.Table, error) {
	wr, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	table, ok, err := wr.GetTable(ctx, tableName)
	if !ok {
		return nil, fmt.Errorf("error: table not found")
	}
	if err != nil {
		return nil, err
	}

	return table, nil
}

func createRow(schema schema.Schema, tags []uint64, vals []types.Value) (row.Row, error) {
	if len(tags) != len(vals) {
		return nil, fmt.Errorf("error: size of tags and vals missaligned")
	}

	var tv = make(row.TaggedValues)
	for i, tag := range tags {
		tv[tag] = vals[i]
	}

	return row.New(nomsType, schema, tv)
}

var setupDrop = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (id int not null primary key, c1 int);"}},
	{commands.SqlCmd{}, []string{"-q", "create index c1_idx on test(c1)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values (1,1),(2,2)"}},
}

func TestDropPk(t *testing.T) {
	t.Run("Drop primary key from table with index", func(t *testing.T) {
		dEnv := dtestutils.CreateTestEnv()
		ctx := context.Background()

		for _, c := range setupDrop {
			c.exec(t, ctx, dEnv)
		}

		table, err := getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		// Get the original index data
		originalMap, err := table.GetNomsIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, originalMap.Empty())

		// Drop the Primary Key
		exitCode := commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test DROP PRIMARY KEY"}, dEnv)
		require.Equal(t, 0, exitCode)

		sch, err := table.GetSchema(ctx)
		assert.NoError(t, err)

		// Assert the new index map is not empty
		newMap, err := table.GetNomsIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))

		// Assert the noms level encoding of the map by ensuring the correct index values are present
		kr1, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(1), types.Int(1)})
		assert.NoError(t, err)

		idx, ok := sch.Indexes().GetByNameCaseInsensitive(indexName)
		assert.True(t, ok)

		full, _, _, err := kr1.ReduceToIndexKeys(idx, nil)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)

		kr2, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(2), types.Int(2)})
		assert.NoError(t, err)

		full, _, _, err = kr2.ReduceToIndexKeys(idx, nil)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestDropPks(t *testing.T) {
	var dropTests = []struct {
		name      string
		setup     []string
		exit      int
		fkIdxName string
	}{
		{
			name: "no error on drop pk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id))",
				"insert into parent values (1,1,1),(2,2,2)",
			},
			exit: 0,
		},
		{
			name: "no error if backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id), key `backup` (id))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if backup index for single FK on compound pk drop",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (age))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (age) references parent (age))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if compound backup index for compound FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (id, age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if compound backup index for compound FK, 3-compound PK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age, name), key `backup` (id, age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if single backup index for single FK, compound primary",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if both several invalid and one valid backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup1` (id, age), key `bad_backup2` (id), key `backup` (age, id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (age) references parent (age))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if one invalid and several valid backup indexes",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (id, age), key `backup1` (age, id, name), key `backup2` (age, id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (age) references parent (age))",
			},
			exit:      0,
			fkIdxName: "backup1",
		},
		{
			name: "perfer unique key",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (id, age), key `backup1` (age, id, name), unique key `backup2` (age, id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (age) references parent (age))",
			},
			exit:      0,
			fkIdxName: "backup2",
		},
		{
			name: "backup index has more columns than pk or fk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, other int, primary key (id, age, name), key `backup` (id, age, other))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "error if FK ref but no backup index for single pk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      1,
			fkIdxName: "id",
		},
		{
			name: "error if FK ref but bad backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id), key `bad_backup2` (age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      1,
			fkIdxName: "id",
		},
		{
			name: "error if misordered compound backup index for FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, constraint `primary` primary key (id), key `backup` (age, id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      1,
			fkIdxName: "id",
		},
		{
			name: "error if incomplete compound backup index for FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, constraint `primary` primary key (age, id), key `backup` (age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (age, id) references parent (age,  id))",
			},
			exit:      1,
			fkIdxName: "ageid",
		},
	}

	for _, tt := range dropTests {
		childName := "child"
		parentName := "parent"
		childFkName := "fk"

		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			ctx := context.Background()

			opts := editor.Options{Deaf: dEnv.DbEaFactory()}
			db := sqle.NewDatabase("dolt", dEnv.DbData(), opts)
			root, _ := dEnv.WorkingRoot(ctx)
			engine, sqlCtx, err := sqle.NewTestEngine(t, dEnv, ctx, db, root)
			require.NoError(t, err)

			for _, query := range tt.setup {
				_, _, err := engine.Query(sqlCtx, query)
				require.NoError(t, err)
			}

			drop := "alter table parent drop primary key"
			_, _, err = engine.Query(sqlCtx, drop)
			switch tt.exit {
			case 1:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}

			if tt.fkIdxName != "" {
				root, _ = db.GetRoot(sqlCtx)
				foreignKeyCollection, err := root.GetForeignKeyCollection(ctx)
				assert.NoError(t, err)

				fk, ok := foreignKeyCollection.GetByNameCaseInsensitive(childFkName)
				assert.True(t, ok)
				assert.Equal(t, childName, fk.TableName)
				assert.Equal(t, tt.fkIdxName, fk.ReferencedTableIndex)

				parent, ok, err := root.GetTable(ctx, parentName)
				assert.NoError(t, err)
				assert.True(t, ok)

				parentSch, err := parent.GetSchema(ctx)
				assert.NoError(t, err)
				err = fk.ValidateReferencedTableSchema(parentSch)
				assert.NoError(t, err)
			}
		})
	}

}
