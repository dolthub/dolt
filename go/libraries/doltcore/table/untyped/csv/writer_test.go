package csv

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

const (
	nameColName  = "name"
	ageColName   = "age"
	titleColName = "title"
	nameColTag   = 0
	ageColTag    = 1
	titleColTag  = 2
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

func TestWriter(t *testing.T) {
	const root = "/"
	const path = "/file.csv"
	const expected = `name,age,title
Bill Billerson,32,Senior Dufus
Rob Robertson,25,Dufus
John Johnson,21,Intern Dufus
`
	info := NewCSVInfo()
	var inCols = []schema.Column{
		{Name: nameColName, Tag: nameColTag, Kind: types.StringKind, IsPartOfPK: true, Constraints: nil},
		{Name: ageColName, Tag: ageColTag, Kind: types.UintKind, IsPartOfPK: false, Constraints: nil},
		{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
	}
	colColl, _ := schema.NewColCollection(inCols...)
	rowSch := schema.SchemaFromCols(colColl)
	rows := []row.Row{
		row.New(types.Format_7_18, rowSch, row.TaggedValues{
			nameColTag:  types.String("Bill Billerson"),
			ageColTag:   types.Uint(32),
			titleColTag: types.String("Senior Dufus")}),
		row.New(types.Format_7_18, rowSch, row.TaggedValues{
			nameColTag:  types.String("Rob Robertson"),
			ageColTag:   types.Uint(25),
			titleColTag: types.String("Dufus")}),
		row.New(types.Format_7_18, rowSch, row.TaggedValues{
			nameColTag:  types.String("John Johnson"),
			ageColTag:   types.Uint(21),
			titleColTag: types.String("Intern Dufus")}),
	}

	_, outSch := untyped.NewUntypedSchema(nameColName, ageColName, titleColName)

	fs := filesys.NewInMemFS(nil, nil, root)
	csvWr, err := OpenCSVWriter(path, fs, outSch, info)

	if err != nil {
		t.Fatal("Could not open CSVWriter", err)
	}

	func() {
		defer csvWr.Close(context.Background())

		for _, row := range rows {
			err := csvWr.WriteRow(context.Background(), row)

			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()

	results, err := fs.ReadFile(path)
	if string(results) != expected {
		t.Errorf(`%s != %s`, results, expected)
	}
}
