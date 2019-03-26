package commands

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"strings"
	"testing"
)

//var UUIDS = []uuid.UUID{
//	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
//	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
//	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
//var Names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
//var Ages = []uint64{32, 25, 21}
//var Titles = []string{"Senior Dufus", "Dufus", ""}
//var MaritalStatus = []bool{true, false, false}

var expectedFieldVals = map[uint64]types.Value{
	dtestutils.IdTag:        expectedId,
	dtestutils.NameTag:      types.String("Bill Billerson"),
	dtestutils.AgeTag:       types.Uint(32),
	dtestutils.IsMarriedTag: types.Bool(true),
	dtestutils.TitleTag:     types.String("Senior Dufus"),
}

var expectedId = types.UUID(uuid.Must(uuid.Parse("11111111-1111-1111-1111-111111111111")))
var tableName = "people"

// Smoke tests, values are printed to console
func TestSqlSelect(t *testing.T) {
	tests := []struct {
		args          []string
		expectedRes   int
	}{
		{[]string{""}, 1},
		{[]string{"-q", "select * from doesnt_exist where age = 32"}, 1},
		{[]string{"-q", "select * from people where age = 32"}, 0},
		{[]string{"-q", "select * from people where title = 'Senior Dufus'"}, 0},
		{[]string{"-q", "select * from people where name = 'Bill Billerson'"}, 0},
		{[]string{"-q", "select * from people where name = 'John Johnson'"}, 0},
		{[]string{"-q", "select * from people where age = 25"}, 0},
		{[]string{"-q", "select * from people where 25 = age"}, 0},
		{[]string{"-q", "select * from people where is_married = false"}, 0},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)

		fmt.Printf("Query is %v\n", test.args)

		commandStr := "dolt sql"
		result := Sql(commandStr, test.args, dEnv)

		if result != test.expectedRes {
			commandLine := commandStr + " " + strings.Join(test.args, " ")
			t.Fatal("Unexpected failure. command", commandLine, "returned", result)
		}

		//if result == 0 {
		//	root, _ := dEnv.WorkingRoot()
		//	tbl, _ := root.GetTable(tableName)
		//	sch := tbl.GetSchema()
		//	row, exists := tbl.GetRowByPKVals(row.TaggedValues{dtestutils.IdTag: expectedId}, sch)
		//
		//	if !exists {
		//		t.Fatal("Could not find row")
		//	}
		//
		//	for i, v := range expectedFieldVals {
		//		val, fld := row.GetColVal(i)
		//
		//		if !val.Equals(v) {
		//			t.Error("Unexpected value for", fld, "expected:", v, "actual:", val)
		//		}
		//	}
		//}
	}
}

func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(rd, wr, false)
	rd.Close()
	wr.Close()

	if err != nil {
		t.Error("Failed to seed initial data", err)
	}

	err = dEnv.PutTableToWorking(*wr.GetMap(), wr.GetSchema(), tableName)

	if err != nil {
		t.Error("Unable to put initial value of table in in mem noms db", err)
	}

	return dEnv
}