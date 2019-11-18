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

package dtestutils

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var UUIDS = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var Names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var Ages = []uint64{32, 25, 21}
var Titles = []string{"Senior Dufus", "Dufus", ""}
var MaritalStatus = []bool{true, false, false}

const (
	IdTag uint64 = iota
	NameTag
	AgeTag
	IsMarriedTag
	TitleTag
	NextTag // leave last
)

var typedColColl, _ = schema.NewColCollection(
	schema.NewColumn("id", IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", NameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("age", AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("title", TitleTag, types.StringKind, false),
)

var TypedSchema = schema.SchemaFromCols(typedColColl)
var UntypedSchema, _ = untyped.UntypeSchema(TypedSchema)
var TypedRows []row.Row
var UntypedRows []row.Row

func init() {
	for i := 0; i < len(UUIDS); i++ {

		taggedVals := row.TaggedValues{
			IdTag:        types.UUID(UUIDS[i]),
			NameTag:      types.String(Names[i]),
			AgeTag:       types.Uint(Ages[i]),
			TitleTag:     types.String(Titles[i]),
			IsMarriedTag: types.Bool(MaritalStatus[i]),
		}

		r, err := row.New(types.Format_7_18, TypedSchema, taggedVals)

		if err != nil {
			panic(err)
		}

		TypedRows = append(TypedRows, r)

		taggedVals = row.TaggedValues{
			IdTag:        types.UUID(uuid.MustParse(UUIDS[i].String())),
			NameTag:      types.String(Names[i]),
			AgeTag:       types.Uint(Ages[i]),
			TitleTag:     types.String(Titles[i]),
			IsMarriedTag: types.Bool(MaritalStatus[i]),
		}

		r, err = row.New(types.Format_7_18, TypedSchema, taggedVals)

		if err != nil {
			panic(err)
		}

		UntypedRows = append(UntypedRows, r)
	}
}

func NewTypedRow(id uuid.UUID, name string, age uint, isMarried bool, title *string) row.Row {
	var titleVal types.Value
	if title != nil {
		titleVal = types.String(*title)
	}

	taggedVals := row.TaggedValues{
		IdTag:        types.UUID(id),
		NameTag:      types.String(name),
		AgeTag:       types.Uint(age),
		IsMarriedTag: types.Bool(isMarried),
		TitleTag:     titleVal,
	}

	r, err := row.New(types.Format_7_18, TypedSchema, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

func AddRowToRoot(dEnv *env.DoltEnv, ctx context.Context, root *doltdb.RootValue, tblName string, r row.Row) (*doltdb.RootValue, error) {

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	m, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	} else {
		me := m.Edit()
		updated, err := me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch)).Map(ctx)

		if err != nil {
			return nil, err
		} else {
			tbl, err = tbl.UpdateRows(ctx, updated)

			if err != nil {
				return nil, err
			} else {
				root, err = root.PutTable(ctx, tblName, tbl)

				if err != nil {
					return nil, err
				} else {
					err = dEnv.UpdateWorkingRoot(context.Background(), root)
					if err != nil {
						return nil, err
					}
					return dEnv.WorkingRoot(ctx)
				}
			}
		}
	}
}

func CreateTestDataTable(typed bool) (*table.InMemTable, schema.Schema) {
	sch := TypedSchema
	rows := TypedRows
	if !typed {
		sch = UntypedSchema
		rows = UntypedRows
	}

	imt := table.NewInMemTable(sch)

	for _, r := range rows {
		err := imt.AppendRow(r)
		if err != nil {
			panic(err)
		}
	}

	return imt, sch
}

// AddColToRows adds a column to all the rows given and returns it. This method relies on the fact that
// noms_row.SetColVal doesn't need a full schema, just one that includes the column being set.
func AddColToRows(t *testing.T, rs []row.Row, tag uint64, val types.Value) []row.Row {
	if types.IsNull(val) {
		return rs
	}

	colColl, err := schema.NewColCollection(schema.NewColumn("unused", tag, val.Kind(), false))
	require.NoError(t, err)
	fakeSch := schema.UnkeyedSchemaFromCols(colColl)

	newRows := make([]row.Row, len(rs))
	for i, r := range rs {
		newRows[i], err = r.SetColVal(tag, val, fakeSch)
		require.NoError(t, err)
	}
	return newRows
}

// Coerces the rows given into the schema given. Only possible if the types are equivalent.
func ConvertToSchema(sch schema.Schema, rs ...row.Row) []row.Row {
	newRows := make([]row.Row, len(rs))
	for i, r := range rs {
		taggedVals := make(row.TaggedValues)
		_, err := r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
			if _, ok := sch.GetAllCols().GetByTag(tag); ok {
				taggedVals[tag] = val
			}
			return false, nil
		})

		if err != nil {
			panic(err)
		}

		newRows[i], err = row.New(types.Format_7_18, sch, taggedVals)

		if err != nil {
			panic(err)
		}
	}
	return newRows
}
