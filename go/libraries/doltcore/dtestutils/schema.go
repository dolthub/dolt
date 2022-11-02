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
	"math"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/google/go-cmp/cmp"
)

// CreateSchema returns a schema from the columns given, panicking on any errors.
func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl := schema.NewColCollection(columns...)
	sch := schema.MustSchemaFromCols(colColl)
	sch.SetCollation(schema.Collation_Default)
	return sch
}

// Creates a row with the schema given, having the values given. Starts at tag 0 and counts up.
func NewRow(sch schema.Schema, values ...types.Value) row.Row {
	taggedVals := make(row.TaggedValues)
	for i := range values {
		taggedVals[uint64(i)] = values[i]
	}
	r, err := row.New(types.Format_Default, sch, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

// AddColumnToSchema returns a new schema by adding the given column to the given schema. Will panic on an invalid
// schema, e.g. tag collision.
// Note the AddColumnToSchema relies on being called from the engine (GMS) to correctly update defaults. Directly calling
// this method in Dolt only adds a new column to schema but does not apply the default.
func AddColumnToSchema(sch schema.Schema, col schema.Column) schema.Schema {
	columns := sch.GetAllCols()
	columns = columns.Append(col)
	newSch := schema.MustSchemaFromCols(columns)
	newSch.SetCollation(sch.GetCollation())
	return newSch
}

// RemoveColumnFromSchema returns a new schema with the given tag missing, but otherwise identical. At least one
// primary column must remain.
func RemoveColumnFromSchema(sch schema.Schema, tagToRemove uint64) schema.Schema {
	var newCols []schema.Column
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if tag != tagToRemove {
			newCols = append(newCols, col)
		}
		return false, nil
	})

	if err != nil {
		panic(err)
	}

	columns := schema.NewColCollection(newCols...)
	newSch := schema.MustSchemaFromCols(columns)
	newSch.SetCollation(sch.GetCollation())
	return newSch
}

// Compares two noms Floats for approximate equality
var FloatComparer = cmp.Comparer(func(x, y types.Float) bool {
	return math.Abs(float64(x)-float64(y)) < .001
})

var TimestampComparer = cmp.Comparer(func(x, y types.Timestamp) bool {
	return x.Equals(y)
})

// MustSchema takes a variable number of columns and returns a schema.
func MustSchema(cols ...schema.Column) schema.Schema {
	hasPKCols := false
	for _, col := range cols {
		if col.IsPartOfPK {
			hasPKCols = true
			break
		}
	}

	colColl := schema.NewColCollection(cols...)

	if !hasPKCols {
		return schema.UnkeyedSchemaFromCols(colColl)
	} else {
		return schema.MustSchemaFromCols(colColl)
	}
}
