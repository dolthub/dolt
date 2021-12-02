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

package untyped

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

// NewUntypedSchema takes an array of field names and returns a schema where the fields use the provided names, are of
// kind types.StringKind, and are not required.
func NewUntypedSchema(colNames ...string) (map[string]uint64, schema.Schema) {
	// TODO: pass PK arg here
	return NewUntypedSchemaWithFirstTag(0, colNames...)
}

func NewUntypedSchemaWithFirstTag(firstTag uint64, colNames ...string) (map[string]uint64, schema.Schema) {
	cols := make([]schema.Column, len(colNames))
	nameToTag := make(map[string]uint64, len(colNames))

	for i, name := range colNames {
		tag := uint64(i) + firstTag
		// We need at least one primary key col, so choose the first one
		isPk := i == 0
		cols[i] = schema.NewColumn(name, tag, types.StringKind, isPk)
		nameToTag[name] = tag
	}

	colColl := schema.NewColCollection(cols...)
	sch := schema.MustSchemaFromCols(colColl)

	return nameToTag, sch
}

// NewRowFromStrings is a utility method that takes a schema for an untyped row, and a slice of strings and uses the strings
// as the field values for the row by converting them to noms type.String
func NewRowFromStrings(nbf *types.NomsBinFormat, sch schema.Schema, valStrs []string) (row.Row, error) {
	allCols := sch.GetAllCols()

	taggedVals := make(row.TaggedValues)
	for i, valStr := range valStrs {
		tag := uint64(i)
		_, ok := allCols.GetByTag(tag)

		if !ok {
			panic("")
		}

		taggedVals[tag] = types.String(valStr)
	}

	return row.New(nbf, sch, taggedVals)
}

// NewRowFromTaggedStrings takes an untyped schema and a map of column tag to string value and returns a row
func NewRowFromTaggedStrings(nbf *types.NomsBinFormat, sch schema.Schema, taggedStrs map[uint64]string) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	for tag, valStr := range taggedStrs {
		taggedVals[tag] = types.String(valStr)
	}

	return row.New(nbf, sch, taggedVals)
}

// UntypeSchema takes a schema and returns a schema with the same columns, but with the types of each of those columns
// as types.StringKind
func UntypeSchema(sch schema.Schema) (schema.Schema, error) {
	var cols []schema.Column
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.Kind = types.StringKind
		col.TypeInfo = typeinfo.StringDefaultType
		cols = append(cols, col)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection(cols...)

	newSch, err := schema.SchemaFromCols(colColl)
	err = newSch.SetPkOrdinals(sch.GetPkOrdinals())
	if err != nil {
		return nil, err
	}
	return newSch, nil
}

// UnkeySchema takes a schema and returns a schema with the same columns and types, but stripped of constraints and
// primary keys. Meant for use in result sets.
func UnkeySchema(sch schema.Schema) (schema.Schema, error) {
	var cols []schema.Column
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.IsPartOfPK = false
		col.Constraints = nil
		cols = append(cols, col)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection(cols...)

	return schema.UnkeyedSchemaFromCols(colColl), nil
}

// UntypeUnkeySchema takes a schema and returns a schema with the same columns, but stripped of constraints and primary
// keys and using only string types. Meant for displaying output and tests.
func UntypeUnkeySchema(sch schema.Schema) (schema.Schema, error) {
	var cols []schema.Column
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.Kind = types.StringKind
		col.IsPartOfPK = false
		col.Constraints = nil
		col.TypeInfo = typeinfo.StringDefaultType
		cols = append(cols, col)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection(cols...)

	return schema.UnkeyedSchemaFromCols(colColl), nil
}

// UntypedSchemaUnion takes an arbitrary number of schemas and provides the union of all of their key and non-key columns.
// The columns will all be of type types.StringKind and and IsPartOfPK will be false for every column, and all of the
// columns will be in the schemas non-key ColumnCollection. Columns that share tags must have compatible types.
func UntypedSchemaUnion(schemas ...schema.Schema) (schema.Schema, error) {
	var allCols []schema.Column

	tags := make(map[uint64]schema.Column)
	for _, sch := range schemas {
		err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if existingCol, ok := tags[tag]; !ok {
				tags[tag] = col
				allCols = append(allCols, col)
			} else if existingCol.Kind != col.Kind {
				// TODO: Need to rethink idea of diffability and compatibility
				return true, schema.ErrColTagCollision
			}

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	allColColl := schema.NewColCollection(allCols...)
	sch, err := schema.SchemaFromCols(allColColl)
	if err != nil {
		return nil, err
	}

	return UntypeSchema(sch)
}
