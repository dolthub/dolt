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

package row

import (
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/valutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrRowNotValid = errors.New("invalid row for current schema")

type Row interface {
	// Returns the noms map key for this row, using the schema provided.
	NomsMapKey(sch schema.Schema) types.LesserValuable

	// Returns the noms map value for this row, using the schema provided.
	NomsMapValue(sch schema.Schema) types.Valuable

	// Iterates over all the columns in the row. Columns that have no value set will not be visited.
	IterCols(cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error)

	// Iterates over all columns in the schema, using the value for the row. Columns that have no value set in this row
	// will still be visited, and receive a nil value.
	IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error)

	// Returns the value for the column with the tag given, and a success bool. The value will be null if the row
	// doesn't contain a value for that tag.
	GetColVal(tag uint64) (types.Value, bool)

	// Sets a value for the column with the tag given, returning a new row with the update.
	SetColVal(tag uint64, val types.Value, sch schema.Schema) (Row, error)

	Format() *types.NomsBinFormat
}

func GetFieldByName(colName string, r Row, sch schema.Schema) (types.Value, bool) {
	col, ok := sch.GetAllCols().GetByName(colName)

	if !ok {
		panic("Requesting column that isn't in the schema. This is a bug. columns should be verified in the schema beforet attempted retrieval.")
	} else {
		return r.GetColVal(col.Tag)
	}
}

func GetFieldByNameWithDefault(colName string, defVal types.Value, r Row, sch schema.Schema) types.Value {
	col, ok := sch.GetAllCols().GetByName(colName)

	if !ok {
		panic("Requesting column that isn't in the schema. This is a bug. columns should be verified in the schema beforet attempted retrieval.")
	} else {
		val, ok := r.GetColVal(col.Tag)

		if !ok {
			return defVal
		}

		return val
	}
}

// IsValid returns whether the row given matches the types and satisfies all the constraints of the schema given.
func IsValid(r Row, sch schema.Schema) (bool, error) {
	column, constraint, err := findInvalidCol(r, sch)

	if err != nil {
		return false, err
	}

	return column == nil && constraint == nil, nil
}

// GetInvalidCol returns the first column in the schema that fails a constraint, or nil if none do.
func GetInvalidCol(r Row, sch schema.Schema) (*schema.Column, error) {
	badCol, _, err := findInvalidCol(r, sch)
	return badCol, err
}

// GetInvalidConstraint returns the failed constraint for the row given (previously identified by IsValid) along with
// the column with that constraint. Note that if there is a problem with the row besides the constraint, the constraint
// return value will be nil.
func GetInvalidConstraint(r Row, sch schema.Schema) (*schema.Column, schema.ColConstraint, error) {
	return findInvalidCol(r, sch)
}

// Returns the first encountered invalid column and its constraint, or nil if the row is valid. Column will always be
// set if the row is invalid. Constraint will be set if the first encountered problem is a constraint failure.
func findInvalidCol(r Row, sch schema.Schema) (*schema.Column, schema.ColConstraint, error) {
	allCols := sch.GetAllCols()

	var badCol *schema.Column
	var badCnst schema.ColConstraint
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val, colSet := r.GetColVal(tag)
		if colSet && !types.IsNull(val) && val.Kind() != col.Kind {
			badCol = &col
			return true, nil
		}

		if !col.TypeInfo.IsValid(val) {
			badCol = &col
			return true, fmt.Errorf(`"%v" is not valid for "%v"`, val, col.TypeInfo.String())
		}

		if len(col.Constraints) > 0 {
			for _, cnst := range col.Constraints {
				if !cnst.SatisfiesConstraint(val) {
					badCol = &col
					badCnst = cnst
					return true, nil
				}
			}
		}

		return false, nil
	})

	return badCol, badCnst, err
}

func AreEqual(row1, row2 Row, sch schema.Schema) bool {
	if row1 == nil && row2 == nil {
		return true
	} else if row1 == nil || row2 == nil {
		return false
	}

	for _, tag := range sch.GetAllCols().Tags {
		val1, _ := row1.GetColVal(tag)
		val2, _ := row2.GetColVal(tag)

		if !valutil.NilSafeEqCheck(val1, val2) {
			return false
		}
	}

	return true
}

func GetTaggedVals(row Row) (TaggedValues, error) {
	taggedVals := make(TaggedValues)
	_, err := row.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		taggedVals[tag] = val
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return taggedVals, nil
}
