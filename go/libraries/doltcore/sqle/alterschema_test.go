// Copyright 2022 Dolthub, Inc.
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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func TestRenameTable(t *testing.T) {
	otherTable := "other"
	cc := schema.NewColCollection(
		schema.NewColumn("id", uint64(100), types.StringKind, true, schema.NotNullConstraint{}),
	)
	otherSch, err := schema.SchemaFromCols(cc)
	require.NoError(t, err)

	tests := []struct {
		name           string
		tableName      string
		newTableName   string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "rename table",
			tableName:      "people",
			newTableName:   "newPeople",
			expectedSchema: dtestutils.TypedSchema,
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:         "table not found",
			tableName:    "notFound",
			newTableName: "newNotfound",
			expectedErr:  doltdb.ErrTableNotFound.Error(),
		},
		{
			name:         "name already in use",
			tableName:    "people",
			newTableName: otherTable,
			expectedErr:  doltdb.ErrTableExists.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			dtestutils.CreateTestTable(t, dEnv, otherTable, otherSch)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := renameTable(ctx, root, tt.tableName, tt.newTableName)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			has, err := updatedRoot.HasTable(ctx, tt.tableName)
			require.NoError(t, err)
			assert.False(t, has)
			newTable, ok, err := updatedRoot.GetTable(ctx, tt.newTableName)
			require.NoError(t, err)
			require.True(t, ok)

			sch, err := newTable.GetSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := newTable.GetNomsRowData(ctx)
			require.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))
				foundRows = append(foundRows, tpl)
				return false, err
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

const tableName = "people"

func TestAddColumnToTable(t *testing.T) {
	tests := []struct {
		name           string
		tag            uint64
		newColName     string
		colKind        types.NomsKind
		nullable       Nullable
		defaultVal     *sql.ColumnDefaultValue
		order          *sql.ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:       "bool column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.IntKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "nullable with nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "")),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "nullable with non-nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "42")),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:       "first order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			order:      &sql.ColumnOrder{First: true},
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:       "middle order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			order:      &sql.ColumnOrder{AfterColumn: "age"},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:        "tag collision",
			tag:         dtestutils.AgeTag,
			newColName:  "newCol",
			colKind:     types.IntKind,
			nullable:    NotNull,
			expectedErr: fmt.Sprintf("Cannot create column newCol, the tag %d was already used in table people", dtestutils.AgeTag),
		},
		{
			name:        "name collision",
			tag:         dtestutils.NextTag,
			newColName:  "age",
			colKind:     types.IntKind,
			nullable:    NotNull,
			defaultVal:  mustStringToColumnDefault("10"),
			expectedErr: "A column with the name age already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			assert.NoError(t, err)

			updatedTable, err := addColumnToTable(ctx, root, tbl, tableName, tt.tag, tt.newColName, typeinfo.FromKind(tt.colKind), tt.nullable, tt.defaultVal, "", tt.order)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			index := sch.Indexes().GetByName(dtestutils.IndexName)
			assert.NotNil(t, index)
			tt.expectedSchema.Indexes().AddIndex(index)
			tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))

				if err != nil {
					return false, err
				}

				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)

			indexRowData, err := updatedTable.GetNomsIndexRowData(ctx, dtestutils.IndexName)
			require.NoError(t, err)
			assert.Greater(t, indexRowData.Len(), uint64(0))
		})
	}
}

func mustStringToColumnDefault(defaultString string) *sql.ColumnDefaultValue {
	def, err := parse.StringToColumnDefaultValue(sql.NewEmptyContext(), defaultString)
	if err != nil {
		panic(err)
	}
	return def
}

func schemaNewColumnWithDefault(name string, tag uint64, kind types.NomsKind, partOfPK bool, defaultVal string, constraints ...schema.ColConstraint) schema.Column {
	col := schema.NewColumn(name, tag, kind, partOfPK, constraints...)
	col.Default = defaultVal
	return col
}

func TestDropColumn(t *testing.T) {
	tests := []struct {
		name           string
		colName        string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "remove int",
			colName:        "age",
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.AgeTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:           "remove string",
			colName:        "title",
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.TitleTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:        "column not found",
			colName:     "not found",
			expectedErr: "column not found",
		},
		{
			name:        "remove primary key col",
			colName:     "id",
			expectedErr: "Cannot drop column in primary key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			require.NoError(t, err)

			updatedTable, err := dropColumn(ctx, tbl, tt.colName)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			originalSch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)
			index := originalSch.Indexes().GetByName(dtestutils.IndexName)
			tt.expectedSchema.Indexes().AddIndex(index)
			tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(dtestutils.TypedSchema, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

func TestDropColumnUsedByIndex(t *testing.T) {
	tests := []struct {
		name           string
		colName        string
		expectedIndex  bool
		expectedSchema schema.Schema
		expectedRows   []row.Row
	}{
		{
			name:           "remove int",
			colName:        "age",
			expectedIndex:  true,
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.AgeTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:           "remove string",
			colName:        "title",
			expectedIndex:  true,
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.TitleTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:           "remove name",
			colName:        "name",
			expectedIndex:  false,
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.NameTag),
			expectedRows:   dtestutils.TypedRows,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			require.NoError(t, err)

			updatedTable, err := dropColumn(ctx, tbl, tt.colName)
			require.NoError(t, err)

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			originalSch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)
			tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)

			index := originalSch.Indexes().GetByName(dtestutils.IndexName)
			assert.NotNil(t, index)
			if tt.expectedIndex {
				tt.expectedSchema.Indexes().AddIndex(index)
				indexRowData, err := updatedTable.GetNomsIndexRowData(ctx, dtestutils.IndexName)
				require.NoError(t, err)
				assert.Greater(t, indexRowData.Len(), uint64(0))
			} else {
				assert.Nil(t, sch.Indexes().GetByName(dtestutils.IndexName))
				_, err := updatedTable.GetNomsIndexRowData(ctx, dtestutils.IndexName)
				assert.Error(t, err)
			}
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(dtestutils.TypedSchema, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
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
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup1` (age, id), key `bad_backup2` (age), key `backup` (id, age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      0,
			fkIdxName: "backup",
		},
		{
			name: "no error if one invalid and several valid backup indexes",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (age, id), key `backup1` (id), key `backup2` (id, age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			exit:      0,
			fkIdxName: "backup1",
		},
		{
			name: "prefer unique key",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (age, id), key `backup1` (id, age, name), unique key `backup2` (id, age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
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

			opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
			db := NewDatabase("dolt", dEnv.DbData(), opts)
			root, _ := dEnv.WorkingRoot(ctx)
			engine, sqlCtx, err := NewTestEngine(t, dEnv, ctx, db, root)
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

func TestNewPkOrdinals(t *testing.T) {
	oldSch := schema.MustSchemaFromCols(
		schema.NewColCollection(
			schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
			schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
			schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
			schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
		),
	)
	err := oldSch.SetPkOrdinals([]int{3, 1})
	require.NoError(t, err)

	tests := []struct {
		name          string
		newSch        schema.Schema
		expPkOrdinals []int
		err           error
	}{
		{
			name: "remove column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{2, 1},
		},
		{
			name: "add column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.NextTag, types.StringKind, false),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{4, 1},
		},
		{
			name: "transpose column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
				),
			),
			expPkOrdinals: []int{4, 1},
		},
		{
			name: "transpose PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{1, 2},
		},
		{
			name: "drop PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
		{
			name: "add PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
					schema.NewColumn("new", dtestutils.NextTag, types.StringKind, true),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
		{
			name: "change PK tag",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.NextTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{3, 1},
		},
		{
			name: "change PK name",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.IsMarriedTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{3, 1},
		},
		{
			name: "changing PK tag and name is the same as dropping a PK",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.NextTag, types.IntKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := modifyPkOrdinals(oldSch, tt.newSch)
			if tt.err != nil {
				require.True(t, errors.Is(err, tt.err))
			} else {
				require.Equal(t, res, tt.expPkOrdinals)
			}
		})
	}
}

func TestModifyColumn(t *testing.T) {
	alteredTypeSch := dtestutils.CreateSchema(
		schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
	)
	ti, err := typeinfo.FromSqlType(sql.MustCreateStringWithDefaults(sqltypes.VarChar, 599))
	require.NoError(t, err)
	newNameColSameTag, err := schema.NewColumnWithTypeInfo("name", dtestutils.NameTag, ti, false, "", false, "", schema.NotNullConstraint{})
	require.NoError(t, err)
	alteredTypeSch2 := dtestutils.CreateSchema(
		schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
		newNameColSameTag,
		schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
	)

	tests := []struct {
		name           string
		existingColumn schema.Column
		newColumn      schema.Column
		order          *sql.ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "column rename",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "remove null constraint",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false),
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "reorder first",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order:          &sql.ColumnOrder{First: true},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "reorder middle",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order:          &sql.ColumnOrder{AfterColumn: "is_married"},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "tag collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedErr:    "two different columns with the same tag",
		},
		{
			name:           "name collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("name", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedErr:    "A column with the name name already exists",
		},
		{
			name:           "type change",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedSchema: alteredTypeSch,
			expectedRows: []row.Row{
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000000"),
					types.String("Bill Billerson"),
					types.Uint(32),
					types.Int(1),
					types.String("Senior Dufus"),
				),
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000001"),
					types.String("John Johnson"),
					types.Uint(25),
					types.Int(0),
					types.String("Dufus"),
				),
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000002"),
					types.String("Rob Robertson"),
					types.Uint(21),
					types.Int(0),
					types.String(""),
				),
			},
		},
		{
			name:           "type change same tag",
			existingColumn: schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
			newColumn:      newNameColSameTag,
			expectedSchema: alteredTypeSch2,
			expectedRows:   dtestutils.TypedRows,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			assert.NoError(t, err)

			opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
			updatedTable, err := modifyColumn(ctx, tbl, tt.existingColumn, tt.newColumn, tt.order, opts)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			index := sch.Indexes().GetByName(dtestutils.IndexName)
			assert.NotNil(t, index)
			tt.expectedSchema.Indexes().AddIndex(index)
			tt.expectedSchema.SetPkOrdinals(sch.GetPkOrdinals())
			tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))

				if err != nil {
					return false, err
				}

				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)

			updatedIndexRows, err := updatedTable.GetNomsIndexRowData(context.Background(), index.Name())
			require.NoError(t, err)
			expectedIndexRows, err := editor.RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
			require.NoError(t, err)
			if uint64(len(foundRows)) != updatedIndexRows.Len() || !updatedIndexRows.Equals(expectedIndexRows) {
				t.Error("index contents are incorrect")
			}
		})
	}
}
