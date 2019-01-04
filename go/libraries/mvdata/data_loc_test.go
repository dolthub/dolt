package mvdata

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/nbf"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/test"
	"reflect"
	"testing"
)

func createRootAndFS() (*doltdb.DoltDB, *doltdb.RootValue, filesys.Filesys) {
	testHomeDir := "/user/bheni"
	workingDir := "/user/bheni/datasets/states"
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	ddb := doltdb.LoadDoltDB(doltdb.InMemDoltDB)
	ddb.WriteEmptyRepo("billy bob", "bigbillieb@fake.horse")

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := ddb.Resolve(cs)
	root := commit.GetRootValue()

	return ddb, root, fs
}

func TestBasics(t *testing.T) {
	tests := []struct {
		dl                   *DataLocation
		expectedFmt          DataFormat
		expectedPath         string
		expectedIsFileType   bool
		expectedReqPK        bool
		expectedMustWrSorted bool
	}{
		{NewDataLocation("table-name", ""), DoltDB, "table-name", false, true, false},
		{NewDataLocation("file.csv", ""), CsvFile, "file.csv", true, false, false},
		{NewDataLocation("file.psv", ""), PsvFile, "file.psv", true, false, false},
		{NewDataLocation("file.nbf", ""), NbfFile, "file.nbf", true, true, true},
	}

	for _, test := range tests {
		if test.expectedFmt != test.dl.Format {
			t.Error(test.dl, "Unexpected format")
		}

		if test.expectedPath != test.dl.Path {
			t.Error("Unexpected path")
		}

		if test.expectedIsFileType != test.dl.IsFileType() {
			t.Error("Unexpected IsFileType result")
		}

		if test.expectedReqPK != test.dl.RequiresPK() {
			t.Error("Unexpected IsFileType result")
		}

		if test.expectedMustWrSorted != test.dl.MustWriteSorted() {
			t.Error("Unexpected IsFileType result")
		}
	}
}

var fakeFields = []*schema.Field{
	schema.NewField("a", types.StringKind, true),
	schema.NewField("b", types.StringKind, true),
}
var fakeSchema *schema.Schema
var imt *table.InMemTable
var imtRows []*table.Row

func init() {
	fakeSchema = schema.NewSchema(fakeFields)
	err := fakeSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))

	if err != nil {
		panic(test.ShouldNeverHappen)
	}

	imtRows = []*table.Row{
		table.NewRow(table.RowDataFromValues(fakeSchema, []types.Value{types.String("a"), types.String("1")})),
		table.NewRow(table.RowDataFromValues(fakeSchema, []types.Value{types.String("b"), types.String("2")})),
		table.NewRow(table.RowDataFromValues(fakeSchema, []types.Value{types.String("c"), types.String("3")})),
	}

	imt = table.NewInMemTableWithData(fakeSchema, imtRows)
}

func TestExists(t *testing.T) {
	testLocations := []*DataLocation{
		NewDataLocation("table-name", ""),
		NewDataLocation("file.csv", ""),
		NewDataLocation("file.psv", ""),
		NewDataLocation("file.nbf", ""),
	}

	ddb, root, fs := createRootAndFS()

	for _, loc := range testLocations {
		if loc.Exists(root, fs) {
			t.Error("Shouldn't exist before creation")
		}

		if loc.Format == DoltDB {
			schVal, _ := noms.MarshalAsNomsValue(ddb.ValueReadWriter(), fakeSchema)
			tbl := doltdb.NewTable(ddb.ValueReadWriter(), schVal, types.NewMap(ddb.ValueReadWriter()))
			root = root.PutTable(ddb, loc.Path, tbl)
		} else {
			fs.WriteFile(loc.Path, []byte("test"))
		}

		if !loc.Exists(root, fs) {
			t.Error("Should already exist after creation")
		}
	}
}

func TestCreateRdWr(t *testing.T) {
	tests := []struct {
		dl          *DataLocation
		expectedRdT reflect.Type
		expectedWrT reflect.Type
	}{
		{NewDataLocation("table-name", ""), reflect.TypeOf((*noms.NomsMapReader)(nil)).Elem(), reflect.TypeOf((*noms.NomsMapCreator)(nil)).Elem()},
		{NewDataLocation("file.csv", ""), reflect.TypeOf((*csv.CSVReader)(nil)).Elem(), reflect.TypeOf((*csv.CSVWriter)(nil)).Elem()},
		{NewDataLocation("file.psv", ""), reflect.TypeOf((*csv.CSVReader)(nil)).Elem(), reflect.TypeOf((*csv.CSVWriter)(nil)).Elem()},
		{NewDataLocation("file.nbf", ""), reflect.TypeOf((*nbf.NBFReader)(nil)).Elem(), reflect.TypeOf((*nbf.NBFWriter)(nil)).Elem()},
	}

	ddb, root, fs := createRootAndFS()

	for _, test := range tests {
		loc := test.dl
		wr, err := loc.CreateOverwritingDataWriter(root, fs, true, fakeSchema)

		if err != nil {
			t.Fatal("Unexpected error creating writer.", err)
		}

		actualWrT := reflect.TypeOf(wr).Elem()
		if actualWrT != test.expectedWrT {
			t.Fatal("Unexpected writer type. Expected:", test.expectedWrT.Name(), "actual:", actualWrT.Name())
		}

		inMemRd := table.NewInMemTableReader(imt)
		_, numBad, pipeErr := table.PipeRows(inMemRd, wr, false)
		wr.Close()

		if numBad != 0 || pipeErr != nil {
			t.Fatal("Failed to write data. bad:", numBad, err)
		}

		if nomsWr, ok := wr.(noms.NomsMapWriteCloser); ok {
			vrw := ddb.ValueReadWriter()
			schVal, err := noms.MarshalAsNomsValue(vrw, nomsWr.GetSchema())

			if err != nil {
				t.Fatal("Unable ta update table")
			}

			tbl := doltdb.NewTable(vrw, schVal, *nomsWr.GetMap())
			root = root.PutTable(ddb, test.dl.Path, tbl)
		}

		rd, _, err := loc.CreateReader(root, fs)

		if err != nil {
			t.Fatal("Unexpected error creating writer", err)
		}

		actualRdT := reflect.TypeOf(rd).Elem()
		if actualRdT != test.expectedRdT {
			t.Error("Unexpected reader type. Expected:", test.expectedRdT.Name(), "actual:", actualRdT.Name())
		}

		rd.Close()
	}
}
