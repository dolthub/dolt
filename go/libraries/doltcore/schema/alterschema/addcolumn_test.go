// Copyright 2019 Liquidata, Inc.
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

package alterschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const tableName = "people"

func TestAddColumnToTable(t *testing.T) {
	tests := []struct {
		name           string
		tag            uint64
		newColName     string
		colKind        types.NomsKind
		nullable       Nullable
		defaultVal     string
		order          *ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:       "string column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.StringKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.StringKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "int column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.IntKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "uint column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.UintKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.UintKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "float column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.FloatKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.FloatKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "bool column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.BoolKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.BoolKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "uuid column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.UUIDKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.UUIDKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "string column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.StringKind,
			nullable:   NotNull,
			defaultVal: `("default")`,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.StringKind, false, `("default")`, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.String("default")),
		},
		{
			name:       "int column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   NotNull,
			defaultVal: "42",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Int(42)),
		},
		{
			name:       "uint column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.UintKind,
			nullable:   NotNull,
			defaultVal: "64",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.UintKind, false, "64", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Uint(64)),
		},
		{
			name:       "float column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.FloatKind,
			nullable:   NotNull,
			defaultVal: "33.33",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.FloatKind, false, "33.33", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Float(33.33)),
		},
		{
			name:       "bool column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.BoolKind,
			nullable:   NotNull,
			defaultVal: "true",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.BoolKind, false, "true", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Bool(true)),
		},
		{
			name:       "uuid column with default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.UUIDKind,
			nullable:   NotNull,
			defaultVal: `"00000000-0000-0000-0000-000000000000"`,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.UUIDKind, false, `"00000000-0000-0000-0000-000000000000"`, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t,
				dtestutils.TypedRows, dtestutils.NextTag, types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))),
		},
		{
			name:       "nullable with nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: "",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "")),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "nullable with non-nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: "42",
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42")),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Int(42)),
		},
		{
			name:       "first order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: "42",
			order:      &ColumnOrder{First: true},
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Int(42)),
		},
		{
			name:       "middle order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: "42",
			order:      &ColumnOrder{After: "age"},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.Int(42)),
		},
		{
			name:        "tag collision",
			tag:         dtestutils.AgeTag,
			newColName:  "newCol",
			colKind:     types.IntKind,
			nullable:    NotNull,
			defaultVal:  "",
			expectedErr: fmt.Sprintf("Cannot create column newCol, the tag %d was already used in table people", dtestutils.AgeTag),
		},
		{
			name:        "name collision",
			tag:         dtestutils.NextTag,
			newColName:  "age",
			colKind:     types.IntKind,
			nullable:    NotNull,
			defaultVal:  "10",
			expectedErr: "A column with the name age already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			assert.NoError(t, err)

			updatedTable, err := AddColumnToTable(ctx, root, tbl, tableName, tt.tag, tt.newColName, typeinfo.FromKind(tt.colKind), tt.nullable, tt.defaultVal, "", tt.order)
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
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetRowData(ctx)
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

			indexRowData, err := updatedTable.GetIndexRowData(ctx, dtestutils.IndexName)
			assert.NoError(t, err)
			assert.Greater(t, indexRowData.Len(), uint64(0))
		})
	}
}

func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(context.Background(), dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(context.Background(), rd, wr, false)
	rd.Close(context.Background())
	wr.Close(context.Background())

	if err != nil {
		t.Error("Failed to seed initial data", err)
	}

	err = dEnv.PutTableToWorking(context.Background(), *wr.GetMap(), wr.GetSchema(), tableName)

	if err != nil {
		t.Error("Unable to put initial value of table in in mem noms db", err)
	}

	return dEnv
}

func schemaNewColumn(name string, tag uint64, kind types.NomsKind, partOfPK bool, defaultVal string, constraints ...schema.ColConstraint) schema.Column {
	col := schema.NewColumn(name, tag, kind, partOfPK, constraints...)
	col.Default = defaultVal
	return col
}
