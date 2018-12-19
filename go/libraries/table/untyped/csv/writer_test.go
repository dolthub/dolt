package csv

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"testing"
)

func TestWriter(t *testing.T) {
	const root = "/"
	const path = "/file.csv"
	const expected = `name,age,title
Bill Billerson,32,Senior Dufus
Rob Robertson,25,Dufus
John Johnson,21,Intern Dufus
`

	info := NewCSVInfo()
	fields := []*schema.Field{
		schema.NewField("name", types.StringKind, true),
		schema.NewField("age", types.UintKind, true),
		schema.NewField("title", types.StringKind, true),
	}
	outSch := untyped.NewUntypedSchema([]string{
		fields[0].NameStr(),
		fields[1].NameStr(),
		fields[2].NameStr(),
	})
	rowSch := schema.NewSchema(fields)
	rows := []*table.Row{
		table.NewRow(table.RowDataFromValues(rowSch, []types.Value{
			types.String("Bill Billerson"),
			types.Uint(32),
			types.String("Senior Dufus")})),
		table.NewRow(table.RowDataFromValues(rowSch, []types.Value{
			types.String("Rob Robertson"),
			types.Uint(25),
			types.String("Dufus")})),
		table.NewRow(table.RowDataFromValues(rowSch, []types.Value{
			types.String("John Johnson"),
			types.Uint(21),
			types.String("Intern Dufus")})),
	}

	fs := filesys.NewInMemFS(nil, nil, root)
	csvWr, err := OpenCSVWriter(path, fs, outSch, info)

	if err != nil {
		t.Fatal("Could not open CSVWriter", err)
	}

	func() {
		defer csvWr.Close()

		for _, row := range rows {
			err := csvWr.WriteRow(row)

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
