// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
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

const (
	TableName = "people"
	IndexName = "idx_name"
)

var typedColColl = schema.NewColCollection(
	schema.NewColumn("id", IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", NameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("age", AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("title", TitleTag, types.StringKind, false),
)

// modified by init()
var TypedSchema = schema.MustSchemaFromCols(typedColColl)

// modified by init()
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

		r, err := row.New(types.Format_Default, TypedSchema, taggedVals)

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

		r, err = row.New(types.Format_Default, TypedSchema, taggedVals)

		if err != nil {
			panic(err)
		}

		UntypedRows = append(UntypedRows, r)
	}

	_, err := TypedSchema.Indexes().AddIndexByColTags(IndexName, []uint64{NameTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
	if err != nil {
		panic(err)
	}
	_, err = UntypedSchema.Indexes().AddIndexByColTags(IndexName, []uint64{NameTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
	if err != nil {
		panic(err)
	}

	_, err = TypedSchema.Checks().AddCheck("test-check", "age < 123", true)
	if err != nil {
		panic(err)
	}
	_, err = UntypedSchema.Checks().AddCheck("test-check", "age < 123", true)
	if err != nil {
		panic(err)
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

	r, err := row.New(types.Format_Default, TypedSchema, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
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

		newRows[i], err = row.New(types.Format_Default, sch, taggedVals)

		if err != nil {
			panic(err)
		}
	}
	return newRows
}

// MustRowData converts a slice of row.TaggedValues into a noms types.Map containing that data.
func MustRowData(t *testing.T, ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, colVals []row.TaggedValues) *types.Map {
	m, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)

	me := m.Edit()
	for _, taggedVals := range colVals {
		r, err := row.New(types.Format_Default, sch, taggedVals)
		require.NoError(t, err)

		me = me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = me.Map(ctx)
	require.NoError(t, err)

	return &m
}

// MustMap contructs a types.Map for a slice of alternating key, value types.Value.
func MustMap(t *testing.T, vrw types.ValueReadWriter, kv ...types.Value) types.Map {
	m, err := types.NewMap(context.Background(), vrw, kv...)
	require.NoError(t, err)
	return m
}

// MustMap contructs a types.Tuple for a slice of types.Values.
func MustTuple(vals ...types.Value) types.Tuple {
	tup, err := types.NewTuple(types.Format_Default, vals...)
	if err != nil {
		panic(err)
	}
	return tup
}
