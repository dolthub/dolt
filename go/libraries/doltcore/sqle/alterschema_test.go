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
	goerrors "errors"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func TestRenameTable(t *testing.T) {
	setup := `
	CREATE TABLE people (
	    id varchar(36) primary key,
	    name varchar(40) not null,
	    age int unsigned,
	    is_married int,
	    title varchar(40),
	    INDEX idx_name (name)
	);
	INSERT INTO people VALUES
		('00000000-0000-0000-0000-000000000000', 'Bill Billerson', 32, 1, 'Senior Dufus'),
		('00000000-0000-0000-0000-000000000001', 'John Johnson', 25, 0, 'Dufus'),
		('00000000-0000-0000-0000-000000000002', 'Rob Robertson', 21, 0, '');
	CREATE TABLE other (c0 int, c1 int);`

	tests := []struct {
		description string
		oldName     string
		newName     string
		expectedErr string
	}{
		{
			description: "rename table",
			oldName:     "people",
			newName:     "newPeople",
		},
		{
			description: "table not found",
			oldName:     "notFound",
			newName:     "newNotfound",
			expectedErr: doltdb.ErrTableNotFound.Error(),
		},
		{
			description: "name already in use",
			oldName:     "people",
			newName:     "other",
			expectedErr: doltdb.ErrTableExists.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			// setup tests
			root, err = ExecuteSql(ctx, dEnv, root, setup)
			require.NoError(t, err)

			schemas, err := doltdb.GetAllSchemas(ctx, root)
			require.NoError(t, err)
			beforeSch := schemas[doltdb.TableName{Name: tt.oldName}]

			updatedRoot, err := renameTable(ctx, root, doltdb.TableName{Name: tt.oldName}, doltdb.TableName{Name: tt.newName})
			if len(tt.expectedErr) > 0 {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			assert.NoError(t, err)
			err = dEnv.UpdateWorkingRoot(ctx, root)
			require.NoError(t, err)

			has, err := updatedRoot.HasTable(ctx, doltdb.TableName{Name: tt.oldName})
			require.NoError(t, err)
			assert.False(t, has)
			has, err = updatedRoot.HasTable(ctx, doltdb.TableName{Name: tt.newName})
			require.NoError(t, err)
			assert.True(t, has)

			schemas, err = doltdb.GetAllSchemas(ctx, updatedRoot)
			require.NoError(t, err)
			require.Equal(t, beforeSch, schemas[doltdb.TableName{Name: tt.newName}])
		})
	}
}

const tableName = "people"

func TestAddColumnToTable(t *testing.T) {
	origRows, sch, err := dtestutils.RowsAndSchema()
	require.NoError(t, err)

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
			expectedSchema: dtestutils.AddColumnToSchema(sch,
				schema.NewColumn("newCol", dtestutils.NextTag, types.IntKind, false)),
			expectedRows: origRows,
		},
		{
			name:       "nullable with nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(sch,
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "")),
			expectedRows: origRows,
		},
		{
			name:       "nullable with non-nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			expectedSchema: dtestutils.AddColumnToSchema(sch,
				schemaNewColumnWithDefault("newCol", dtestutils.NextTag, types.IntKind, false, "42")),
			expectedRows: addColToRows(t, origRows, dtestutils.NextTag, types.NullValue),
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
			expectedRows: addColToRows(t, origRows, dtestutils.NextTag, types.NullValue),
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
			expectedRows: addColToRows(t, origRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:        "tag collision",
			tag:         dtestutils.AgeTag,
			newColName:  "newCol",
			colKind:     types.IntKind,
			nullable:    NotNull,
			expectedErr: fmt.Sprintf("cannot create column newCol on table people, the tag %d was already used in table people", dtestutils.AgeTag),
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
			ctx := context.Background()
			dEnv, err := makePeopleTable(ctx, dtestutils.CreateTestEnv())
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
			assert.True(t, ok)
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
			_, err = tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)
		})
	}
}

func makePeopleTable(ctx context.Context, dEnv *env.DoltEnv) (*env.DoltEnv, error) {
	_, sch, err := dtestutils.RowsAndSchema()
	if err != nil {
		return nil, err
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := durable.NewEmptyPrimaryIndex(ctx, root.VRW(), root.NodeStore(), sch)
	if err != nil {
		return nil, err
	}
	indexes, err := durable.NewIndexSetWithEmptyIndexes(ctx, root.VRW(), root.NodeStore(), sch)
	if err != nil {
		return nil, err
	}
	tbl, err := doltdb.NewTable(ctx, root.VRW(), root.NodeStore(), sch, rows, indexes, nil)
	if err != nil {
		return nil, err
	}
	root, err = root.PutTable(ctx, doltdb.TableName{Name: tableName}, tbl)
	if err != nil {
		return nil, err
	}
	if err = dEnv.UpdateWorkingRoot(ctx, root); err != nil {
		return nil, err
	}
	return dEnv, nil
}

func mustStringToColumnDefault(defaultString string) *sql.ColumnDefaultValue {
	def, err := planbuilder.StringToColumnDefaultValue(sql.NewEmptyContext(), defaultString)
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

func TestDropPks(t *testing.T) {
	var dropTests = []struct {
		name        string
		setup       []string
		expectedErr *errors.Kind
		fkIdxName   string
	}{
		{
			name: "no error on drop pk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id))",
				"insert into parent values (1,1,1),(2,2,2)",
			},
		},
		{
			name: "no error if backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id), key `backup` (id))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (id) references parent (id))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if backup index for single FK on compound pk drop",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (age))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (age) references parent (age))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if compound backup index for compound FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (id, age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if compound backup index for compound FK, 3-compound PK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age, name), key `backup` (id, age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if single backup index for single FK, compound primary",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `backup` (id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if both several invalid and one valid backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup1` (age, id), key `bad_backup2` (age), key `backup` (id, age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			fkIdxName: "backup",
		},
		{
			name: "no error if one invalid and several valid backup indexes",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (age, id), key `backup1` (id), key `backup2` (id, age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			fkIdxName: "backup1",
		},
		{
			name: "prefer unique key",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id, age), key `bad_backup` (age, id), key `backup1` (id, age, name), unique key `backup2` (id, age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			fkIdxName: "backup2",
		},
		{
			name: "backup index has more columns than pk or fk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, other int, primary key (id, age, name), key `backup` (id, age, other))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id, age) references parent (id, age))",
			},
			fkIdxName: "backup",
		},
		{
			name: "error if FK ref but no backup index for single pk",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id))",
				"create table child (id int, name varchar(1), age int, primary key (id), constraint `fk` foreign key (id) references parent (id))",
			},
			expectedErr: sql.ErrCantDropIndex,
			fkIdxName:   "id",
		},
		{
			name: "error if FK ref but bad backup index",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, primary key (id), key `bad_backup2` (age))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			expectedErr: sql.ErrCantDropIndex,
			fkIdxName:   "id",
		},
		{
			name: "error if misordered compound backup index for FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, constraint `primary` primary key (id), key `backup` (age, id))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (id) references parent (id))",
			},
			expectedErr: sql.ErrCantDropIndex,
			fkIdxName:   "id",
		},
		{
			name: "error if incomplete compound backup index for FK",
			setup: []string{
				"create table parent (id int, name varchar(1), age int, constraint `primary` primary key (age, id), key `backup` (age, name))",
				"create table child (id int, name varchar(1), age int, constraint `fk` foreign key (age, id) references parent (age,  id))",
			},
			expectedErr: sql.ErrCantDropIndex,
			fkIdxName:   "ageid",
		},
	}

	for _, tt := range dropTests {
		childName := "child"
		parentName := "parent"
		childFkName := "fk"

		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()
			tmpDir, err := dEnv.TempTableFilesDir()
			require.NoError(t, err)
			opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
			db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), opts)
			require.NoError(t, err)

			root, _ := dEnv.WorkingRoot(ctx)
			engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
			require.NoError(t, err)

			for _, query := range tt.setup {
				_, _, _, err := engine.Query(sqlCtx, query)
				require.NoError(t, err)
			}

			drop := "alter table parent drop primary key"
			_, iter, _, err := engine.Query(sqlCtx, drop)
			require.NoError(t, err)

			err = drainIter(sqlCtx, iter)
			if tt.expectedErr != nil {
				require.Error(t, err)
				assert.True(t, tt.expectedErr.Is(err), "Expected error of type %s but got %s", tt.expectedErr, err)
			} else {
				require.NoError(t, err)
			}

			if tt.fkIdxName != "" {
				root, _ = db.GetRoot(sqlCtx)
				foreignKeyCollection, err := root.GetForeignKeyCollection(ctx)
				assert.NoError(t, err)

				fk, ok := foreignKeyCollection.GetByNameCaseInsensitive(childFkName)
				assert.True(t, ok)
				assert.Equal(t, childName, fk.TableName.Name)
				if tt.fkIdxName != "" && fk.ReferencedTableIndex != "" {
					assert.Equal(t, tt.fkIdxName, fk.ReferencedTableIndex)
				}

				parent, ok, err := root.GetTable(ctx, doltdb.TableName{Name: parentName})
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
				require.True(t, goerrors.Is(err, tt.err))
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
	ti, err := typeinfo.FromSqlType(gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 599))
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
			expectedErr:    "two different columns with the same name exist",
		},
		{
			name:           "type change",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedSchema: alteredTypeSch,
		},
		{
			name:           "type change same tag",
			existingColumn: schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
			newColumn:      newNameColSameTag,
			expectedSchema: alteredTypeSch2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv, err := makePeopleTable(ctx, dtestutils.CreateTestEnv())
			require.NoError(t, err)
			defer dEnv.DoltDB(ctx).Close()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
			assert.NoError(t, err)
			updatedTable, err := modifyColumn(ctx, tbl, tt.existingColumn, tt.newColumn, tt.order)
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
			err = tt.expectedSchema.SetPkOrdinals(sch.GetPkOrdinals())
			require.NoError(t, err)
			_, err = tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)
		})
	}
}

// addColToRows adds a column to all the rows given and returns it. This method relies on the fact that
// noms_row.SetColVal doesn't need a full schema, just one that includes the column being set.
func addColToRows(t *testing.T, rs []row.Row, tag uint64, val types.Value) []row.Row {
	if types.IsNull(val) {
		return rs
	}

	colColl := schema.NewColCollection(schema.NewColumn("unused", tag, val.Kind(), false))
	fakeSch := schema.UnkeyedSchemaFromCols(colColl)

	newRows := make([]row.Row, len(rs))
	var err error
	for i, r := range rs {
		newRows[i], err = r.SetColVal(tag, val, fakeSch)
		require.NoError(t, err)
	}
	return newRows
}
