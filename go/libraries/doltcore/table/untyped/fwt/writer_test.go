package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
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
		fwtTransform pipeline.TransformFunc
		expectedOut  string
	}{
		{pipeline.NewRowTransformer("hash when too long fwt transform", fwtTr.Transform), expected},
		{asTr.TransformToFWT, autoSizingExpected},
	}

	for _, test := range tests {
		imt := table.NewInMemTableWithData(testRowSch, testRows)
		rd := table.NewInMemTableReader(imt)

		mapping, _ := schema.NewInferredMapping(testRowSch, sch)
		rconv, _ := table.NewRowConverter(mapping)
		convTr := pipeline.NewRowTransformer("Field Mapping Transform", pipeline.GetRowConvTransformFunc(rconv))

		fs := filesys.NewInMemFS(nil, nil, root)
		tWr, err := OpenTextWriter(path, fs, sch, "")

		if err != nil {
			t.Fatal("Could not open FWTWriter", err)
		}

		badRowCB := func(_ *pipeline.TransformRowFailure) (quit bool) {
			return true
		}

		transforms := pipeline.NewTransformCollection(
			pipeline.NamedTransform{Name: "convert", Func: convTr},
			pipeline.NamedTransform{Name: "fwt", Func: test.fwtTransform})
		p, start := pipeline.NewAsyncPipeline(rd, transforms, tWr, badRowCB)
		start()

		p.Wait()
		tWr.Close()

		results, err := fs.ReadFile(path)
		if string(results) != test.expectedOut {
			t.Errorf("\n%s != \n%s", results, expected)
		}
	}
}
