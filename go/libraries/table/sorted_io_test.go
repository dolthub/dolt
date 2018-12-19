package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"testing"
)

var untypedFields = []*schema.Field{
	schema.NewField("number", types.StringKind, true),
	schema.NewField("name", types.StringKind, true),
}

var typedFields = []*schema.Field{
	schema.NewField("number", types.UintKind, true),
	schema.NewField("name", types.StringKind, true),
}

var untypedSch = schema.NewSchema(untypedFields)
var typedSch = schema.NewSchema(typedFields)

func init() {
	untypedSch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
	typedSch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
}

var untypedRows = []*Row{
	NewRow(RowDataFromValues(untypedSch, []types.Value{
		types.String("9"),
		types.String("Nine")})),
	NewRow(RowDataFromValues(untypedSch, []types.Value{
		types.String("8"),
		types.String("Eight")})),
	NewRow(RowDataFromValues(untypedSch, []types.Value{
		types.String("10"),
		types.String("Ten")})),
}
var typedRows = []*Row{
	NewRow(RowDataFromValues(typedSch, []types.Value{
		types.Uint(9),
		types.String("Nine")})),
	NewRow(RowDataFromValues(typedSch, []types.Value{
		types.Uint(8),
		types.String("Eight")})),
	NewRow(RowDataFromValues(typedSch, []types.Value{
		types.Uint(10),
		types.String("Ten")})),
}

func TestSortingReader(t *testing.T) {
	untypedResults := testSortingReader(t, untypedSch, untypedRows)
	typedResults := testSortingReader(t, typedSch, typedRows)

	if !GetPKFromRow(untypedResults[0]).Equals(types.String("10")) ||
		!GetPKFromRow(untypedResults[1]).Equals(types.String("8")) ||
		!GetPKFromRow(untypedResults[2]).Equals(types.String("9")) {
		t.Error("Unexpected untyped ordering")
	}

	if !GetPKFromRow(typedResults[0]).Equals(types.Uint(8)) ||
		!GetPKFromRow(typedResults[1]).Equals(types.Uint(9)) ||
		!GetPKFromRow(typedResults[2]).Equals(types.Uint(10)) {
		t.Error("Unexpected typed ordering")
	}
}

func testSortingReader(t *testing.T, sch *schema.Schema, rows []*Row) []*Row {
	imt := NewInMemTableWithData(sch, rows)
	imttRd := NewInMemTableReader(imt)

	sr, goodRows, badRows, err := NewSortingTableReaderByPK(imttRd, true)

	if err != nil {
		t.Fatal("Failed to create SortingTableReader")
	}

	if goodRows != 3 && badRows != 0 {
		t.Error("good/bad row count does not match expectations.")
	}

	var results []*Row
	func() {
		defer func() {
			err := sr.Close()

			if err != nil {
				t.Error("Failed to close sorted reader", err)
			}
		}()

		var err error
		results, badRows, err = ReadAllRows(sr, true)

		if err != nil {
			t.Fatal("Failed to read all rows")
		}

		if badRows != 0 {
			t.Error("Num bad rows does not match expectation")
		}
	}()

	return results
}
