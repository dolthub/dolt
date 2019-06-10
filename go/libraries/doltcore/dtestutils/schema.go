package dtestutils

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"math"
)

// CreateSchema returns a schema from the columns given, panicking on any errors.
func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.SchemaFromCols(colColl)
}

// AddColumnToSchema returns a new schema by adding the given column to the given schema. Will panic on an invalid
// schema, e.g. tag collision.
func AddColumnToSchema(sch schema.Schema, col schema.Column) schema.Schema {
	columns := sch.GetAllCols()
	columns, err := columns.Append(col)
	if err != nil {
		panic(err)
	}
	return schema.SchemaFromCols(columns)
}

// RemoveColumnFromSchema returns a new schema with the given tag missing, but otherwise identical. At least one
// primary column must remain.
func RemoveColumnFromSchema(sch schema.Schema, tagToRemove uint64) schema.Schema {
	var newCols []schema.Column
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if tag != tagToRemove {
			newCols = append(newCols, col)
		}
		return false
	})

	columns, err := schema.NewColCollection(newCols...)
	if err != nil {
		panic(err)
	}
	return schema.SchemaFromCols(columns)
}

// Compares two noms Floats for approximate equality
var FloatComparer = cmp.Comparer(func(x, y types.Float) bool {
	return math.Abs(float64(x)-float64(y)) < .001
})

