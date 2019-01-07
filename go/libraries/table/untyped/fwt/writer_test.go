package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"testing"
)

const root = "/"
const path = "/file.fwt"
const expected = "" +
	"Bill Billerson32#####\n" +
	"Rob Robertson 25Dufus\n" +
	"John Johnson  21#####\n"

const autoSizingExpected = "" +
	"Bill Billerson32Senior Dufus\n" +
	"Rob Robertson 25Dufus       \n" +
	"John Johnson  21Intern Dufus\n"

var testFields = []*schema.Field{
	schema.NewField("name", types.StringKind, true),
	schema.NewField("age", types.UintKind, true),
	schema.NewField("title", types.StringKind, true),
}
var testRowSch = schema.NewSchema(testFields)
var testRows = []*table.Row{
	table.NewRow(table.RowDataFromValues(testRowSch, []types.Value{
		types.String("Bill Billerson"),
		types.Uint(32),
		types.String("Senior Dufus")})),
	table.NewRow(table.RowDataFromValues(testRowSch, []types.Value{
		types.String("Rob Robertson"),
		types.Uint(25),
		types.String("Dufus")})),
	table.NewRow(table.RowDataFromValues(testRowSch, []types.Value{
		types.String("John Johnson"),
		types.Uint(21),
		types.String("Intern Dufus")})),
}

var testRowConv, _ = untyped.TypedToUntypedRowConverter(testRowSch)

func TestAutoSizing(t *testing.T) {
	sch := untyped.NewUntypedSchema([]string{"name", "age", "title"})
	asTr := NewAutoSizingFWTTransformer(sch, HashFillWhenTooLong, 0)

	fwtSch, _ := NewFWTSchema(sch, map[string]int{"name": 14, "age": 2, "title": 5})
	fwtTr := NewFWTTransformer(fwtSch, HashFillWhenTooLong)

	tests := []struct {
		fwtTransform table.TransformFunc
		expectedOut  string
	}{
		{table.NewRowTransformer("hash when too long fwt transform", fwtTr.Transform), expected},
		{asTr.TransformToFWT, autoSizingExpected},
	}

	for _, test := range tests {
		imt := table.NewInMemTableWithData(testRowSch, testRows)
		rd := table.NewInMemTableReader(imt)

		mapping, _ := schema.NewInferredMapping(testRowSch, sch)
		rconv, _ := table.NewRowConverter(mapping)
		convTr := table.NewRowTransformer("Field Mapping Transform", rconv.TransformRow)

		fs := filesys.NewInMemFS(nil, nil, root)
		tWr, err := OpenTextWriter(path, fs, sch, "")

		if err != nil {
			t.Fatal("Could not open FWTWriter", err)
		}

		badRowCB := func(transfName string, row *table.Row, errDetails string) (quit bool) {
			return true
		}

		transforms := table.NewTransformCollection(
			table.NamedTransform{Name: "convert", Func: convTr},
			table.NamedTransform{Name: "fwt", Func: test.fwtTransform})
		pipeline, start := table.NewAsyncPipeline(rd, transforms, tWr, badRowCB)
		start()

		pipeline.Wait()
		tWr.Close()

		results, err := fs.ReadFile(path)
		if string(results) != test.expectedOut {
			t.Errorf("\n%s != \n%s", results, expected)
		}
	}
}
