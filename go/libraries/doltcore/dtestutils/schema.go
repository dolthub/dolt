package dtestutils

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// CreateSchema returns a schema from the columns given, panicking on any errors.
func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.SchemaFromCols(colColl)
}

// Creates a row with the schema given, having the values given. Starts at tag 0 and counts up.
func NewRow(sch schema.Schema, values ...types.Value) row.Row {
	taggedVals := make(row.TaggedValues)
	for i := range values {
		taggedVals[uint64(i)] = values[i]
	}
	return row.New(sch, taggedVals)
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

// CreateTestTable creates a new test table with the name, schema, and rows given.
func CreateTestTable(t *testing.T, dEnv *env.DoltEnv, tableName string, sch schema.Schema, rs ...row.Row) {
	imt := table.NewInMemTable(sch)

	for _, r := range rs {
		imt.AppendRow(r)
	}

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(context.Background(), dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(context.Background(), rd, wr, false)
	rd.Close(context.Background())
	wr.Close(context.Background())

	require.Nil(t, err, "Failed to seed initial data")

	err = dEnv.PutTableToWorking(context.Background(), *wr.GetMap(), wr.GetSchema(), tableName)
	require.Nil(t, err, "Unable to put initial value of table in in-mem noms db")
}
