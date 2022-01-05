// Copyright 2020 Dolthub, Inc.
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
	"fmt"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/utils/set"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// Creates a new schema for a result set specified by the given pairs of column names and types. Column names are
// strings, types are NomsKinds.
func NewResultSetSchema(colNamesAndTypes ...interface{}) schema.Schema {
	if len(colNamesAndTypes)%2 != 0 {
		panic("Non-even number of inputs passed to NewResultSetSchema")
	}

	cols := make([]schema.Column, len(colNamesAndTypes)/2)
	for i := 0; i < len(colNamesAndTypes); i += 2 {
		name := colNamesAndTypes[i].(string)
		nomsKind := colNamesAndTypes[i+1].(types.NomsKind)
		cols[i/2] = schema.NewColumn(name, uint64(i/2), nomsKind, false)
	}

	collection := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(collection)
}

// Creates a new row for a result set specified by the given values
func NewResultSetRow(colVals ...types.Value) row.Row {
	taggedVals := make(row.TaggedValues)
	cols := make([]schema.Column, len(colVals))
	for i := 0; i < len(colVals); i++ {
		taggedVals[uint64(i)] = colVals[i]
		nomsKind := colVals[i].Kind()
		cols[i] = schema.NewColumn(fmt.Sprintf("%v", i), uint64(i), nomsKind, false)
	}

	collection := schema.NewColCollection(cols...)
	sch := schema.UnkeyedSchemaFromCols(collection)

	r, err := row.New(types.Format_Default, sch, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

// NewRow creates a new row with the values given, using ascending tag numbers starting at 0.
// Uses the first value as the primary key.
func NewRow(colVals ...types.Value) row.Row {
	return NewRowWithPks(colVals[0:1], colVals[1:]...)
}

// NewRowWithPks creates a new row with the values given, using ascending tag numbers starting at 0.
func NewRowWithPks(pkColVals []types.Value, nonPkVals ...types.Value) row.Row {
	var cols []schema.Column
	taggedVals := make(row.TaggedValues)
	var tag int64

	for _, val := range pkColVals {
		var constraints []schema.ColConstraint
		constraints = append(constraints, schema.NotNullConstraint{})
		cols = append(cols, schema.NewColumn(strconv.FormatInt(tag, 10), uint64(tag), val.Kind(), true, constraints...))
		taggedVals[uint64(tag)] = val
		tag++
	}

	for _, val := range nonPkVals {
		cols = append(cols, schema.NewColumn(strconv.FormatInt(tag, 10), uint64(tag), val.Kind(), false))
		taggedVals[uint64(tag)] = val
		tag++
	}

	colColl := schema.NewColCollection(cols...)
	sch := schema.MustSchemaFromCols(colColl)

	r, err := row.New(types.Format_Default, sch, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

// NewRowWithSchema creates a new row with the using the provided schema.
func NewRowWithSchema(sch schema.Schema, vals ...types.Value) row.Row {
	tv := make(row.TaggedValues)
	var i int
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tv[tag] = vals[i]
		i++
		return false, nil
	})

	r, err := row.New(types.Format_Default, sch, tv)
	if err != nil {
		panic(err)
	}

	return r
}

// NewSchema creates a new schema with the pairs of column names and types given.
// Uses the first column as the primary key.
func NewSchema(colNamesAndTypes ...interface{}) schema.Schema {
	return NewSchemaForTable("", colNamesAndTypes...)
}

// NewSchemaForTable creates a new schema for the table with the name given with the pairs of column names and types
// given. Uses the first column as the primary key.
func NewSchemaForTable(tableName string, colNamesAndTypes ...interface{}) schema.Schema {
	if len(colNamesAndTypes)%2 != 0 {
		panic("Non-even number of inputs passed to NewSchema")
	}

	// existingTags *set.Uint64Set, tableName string, existingColKinds []types.NomsKind, newColName string, newColKind types.NomsKind
	nomsKinds := make([]types.NomsKind, 0)
	tags := set.NewUint64Set(nil)

	cols := make([]schema.Column, len(colNamesAndTypes)/2)
	for i := 0; i < len(colNamesAndTypes); i += 2 {
		name := colNamesAndTypes[i].(string)
		nomsKind := colNamesAndTypes[i+1].(types.NomsKind)

		tag := schema.AutoGenerateTag(tags, tableName, nomsKinds, name, nomsKind)
		tags.Add(tag)
		nomsKinds = append(nomsKinds, nomsKind)

		isPk := i/2 == 0
		var constraints []schema.ColConstraint
		if isPk {
			constraints = append(constraints, schema.NotNullConstraint{})
		}
		cols[i/2] = schema.NewColumn(name, tag, nomsKind, isPk, constraints...)
	}

	colColl := schema.NewColCollection(cols...)
	return schema.MustSchemaFromCols(colColl)
}

// Returns the logical concatenation of the schemas and rows given, rewriting all tag numbers to begin at zero. The row
// returned will have a new schema identical to the result of compressSchema.
func ConcatRows(schemasAndRows ...interface{}) row.Row {
	if len(schemasAndRows)%2 != 0 {
		panic("Non-even number of inputs passed to concatRows")
	}

	taggedVals := make(row.TaggedValues)
	cols := make([]schema.Column, 0)
	var itag uint64
	for i := 0; i < len(schemasAndRows); i += 2 {
		sch := schemasAndRows[i].(schema.Schema)
		r := schemasAndRows[i+1].(row.Row)
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			val, ok := r.GetColVal(tag)
			if ok {
				taggedVals[itag] = val
			}

			col.Tag = itag
			cols = append(cols, col)
			itag++

			return false
		})
	}

	colCol := schema.NewColCollection(cols...)
	r, err := row.New(types.Format_Default, schema.UnkeyedSchemaFromCols(colCol), taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}
