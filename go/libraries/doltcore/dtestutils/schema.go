package dtestutils

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"math"
)

func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.SchemaFromCols(colColl)
}

func AddColumnToSchema(sch schema.Schema, col schema.Column) schema.Schema {
	columns := sch.GetAllCols()
	columns, err := columns.Append(col)
	if err != nil {
		panic(err)
	}
	return schema.SchemaFromCols(columns)
}

// Compares two noms Floats for approximate equality
var FloatComparer = cmp.Comparer(func(x, y types.Float) bool {
	return math.Abs(float64(x)-float64(y)) < .001
})

