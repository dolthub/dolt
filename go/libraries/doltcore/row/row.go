package row

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/valutil"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
)

var ErrRowNotValid = errors.New("invalid row for current schema")

type Row interface {
	// Returns the noms map key for this row, using the schema provided.
	NomsMapKey(sch schema.Schema) types.LesserValuable

	// Returns the noms map value for this row, using the schema provided.
	NomsMapValue(sch schema.Schema) types.Valuable

	// Iterates over all the columns in the row. Columns that have no value set will not be visited.
	IterCols(cb func(tag uint64, val types.Value) (stop bool)) bool

	// Iterates over all columns in the schema, using the value for the row. Columns that have no value set in this row
	// will still be visited, and receive a nil value.
	IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool)) bool

	// Returns the value for the column with the tag given, and a success bool. The value will be null if the row
	// doesn't contain a value for that tag.
	GetColVal(tag uint64) (types.Value, bool)

	// Sets a value for the column with the tag given, returning a new row with the update.
	SetColVal(tag uint64, val types.Value, sch schema.Schema) (Row, error)
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
func IsValid(r Row, sch schema.Schema) bool {
	column, constraint := findInvalidCol(r, sch)
	return column == nil && constraint == nil
}

// GetInvalidCol returns the first column in the schema that fails a constraint, or nil if none do.
func GetInvalidCol(r Row, sch schema.Schema) *schema.Column {
	badCol, _ := findInvalidCol(r, sch)
	return badCol
}

// GetInvalidConstraint returns the failed constraint for the row given (previously identified by IsValid) along with
// the column with that constraint. Note that if there is a problem with the row besides the constraint, the constraint
// return value will be nil.
func GetInvalidConstraint(r Row, sch schema.Schema) (*schema.Column, schema.ColConstraint) {
	return findInvalidCol(r, sch)
}

// Returns the first encountered invalid column and its constraint, or nil if the row is valid. Column will always be
// set if the row is invalid. Constraint will be set if the first encountered problem is a constraint failure.
func findInvalidCol(r Row, sch schema.Schema) (*schema.Column, schema.ColConstraint) {
	allCols := sch.GetAllCols()

	var badCol *schema.Column
	var badCnst schema.ColConstraint
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		val, colSet := r.GetColVal(tag)
		if colSet && !types.IsNull(val) && val.Kind() != col.Kind {
			badCol = &col
			return true
		}

		if len(col.Constraints) > 0 {
			for _, cnst := range col.Constraints {
				if !cnst.SatisfiesConstraint(val) {
					badCol = &col
					badCnst = cnst
					return true
				}
			}
		}

		return false
	})

	return badCol, badCnst
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

func GetTaggedVals(row Row) TaggedValues {
	taggedVals := make(TaggedValues)
	row.IterCols(func(tag uint64, val types.Value) (stop bool) {
		taggedVals[tag] = val
		return false
	})

	return taggedVals
}
