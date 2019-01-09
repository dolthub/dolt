package nbf

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"testing"
)

var typedSchema = schema.NewSchema([]*schema.Field{
	schema.NewField("id", types.UUIDKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("age", types.UintKind, true),
	schema.NewField("title", types.StringKind, false),
})

func init() {
	typedSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
}

var uuids = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var ages = []uint{32, 25, 21}
var titles = []string{"Senior Dufus", "Dufus", ""}

func createRows() []*table.Row {
	rows := make([]*table.Row, len(names))
	for i := 0; i < len(names); i++ {
		rowValMap := map[string]types.Value{
			"id":    types.UUID(uuids[i]),
			"name":  types.String(names[i]),
			"age":   types.Uint(ages[i]),
			"title": types.String(titles[i]),
		}
		rows[i] = table.NewRow(table.RowDataFromValMap(typedSchema, rowValMap))
	}

	return rows
}

func TestNBFReadWrite(t *testing.T) {
	ReadBufSize = 128
	WriteBufSize = 128

	path := "/file.nbf"
	fs := filesys.NewInMemFS([]string{"/"}, nil, "/")
	rows := createRows()

	nbfWr, err := OpenNBFWriter(path, fs, typedSchema)

	if err != nil {
		t.Fatal("Could create file")
	}

	func() {
		defer func() {
			err := nbfWr.Close()
			if err != nil {
				t.Fatal("Failed to close.")
			}
		}()

		for i := 0; i < len(rows); i++ {
			err = nbfWr.WriteRow(rows[i])

			if err != nil {
				t.Fatal("Failed to write row.", err)
			}
		}
	}()

	nbfRd, err := OpenNBFReader(path, fs)

	if err != nil {
		t.Fatal("Failed to open reader")
	}

	var numBad int
	var results []*table.Row
	func() {
		defer func() {
			err := nbfRd.Close()

			if err != nil {
				t.Fatal("Close Failure")
			}
		}()

		results, numBad, err = table.ReadAllRows(nbfRd, true)
	}()

	if numBad != 0 {
		t.Error("Unexpected bad rows")
	}

	if err != nil {
		t.Fatal("Error reading")
	}

	if len(results) != len(rows) {
		t.Error("Unexpected row count")
	}

	for i := 0; i < len(rows); i++ {
		row := rows[i]
		resRow := results[i]

		if !table.RowsEqualIgnoringSchema(row, resRow) {
			t.Error(table.RowFmt(row), "!=", table.RowFmt(resRow))
		}
	}
}
