package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/stretchr/testify/assert"
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

var tableName = "people"

// Smoke tests, values are printed to console
func TestSqlSelect(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"select * from doesnt_exist where age = 32", 1},
		{"select * from people", 0},
		{"select * from people where age = 32", 0},
		{"select * from people where title = 'Senior Dufus'", 0},
		{"select * from people where name = 'Bill Billerson'", 0},
		{"select * from people where name = 'John Johnson'", 0},
		{"select * from people where age = 25", 0},
		{"select * from people where 25 = age", 0},
		{"select * from people where is_married = false", 0},
		{"select * from people where age < 30", 0},
		{"select * from people where age > 24", 0},
		{"select * from people where age >= 25", 0},
		{"select * from people where name <= 'John Johnson'", 0},
		{"select * from people where name <> 'John Johnson'", 0},
		{"select age, is_married from people where name <> 'John Johnson'", 0},
		{"select age, is_married from people where name <> 'John Johnson' limit 1", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)

			args := []string{"-q", test.query}

			commandStr := "dolt sql"
			result := Sql(commandStr, args, dEnv)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

func TestBadInput(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectedRes int
	}{
		{"no input", []string{""}, 1},
		{"no query", []string{"-q", ""}, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)

			commandStr := "dolt sql"
			result := Sql(commandStr, test.args, dEnv)
			assert.Equal(t, test.expectedRes, result)
		})
	}
}

// Tests of the create table SQL syntax, mostly a smoke test for errors in the command line handler. Most tests of
// create table SQL syntax are in the sql package.
func TestCreateTable(t *testing.T) {
	tests := []struct {
		query       string
		expectedRes int
	}{
		{"create table people (id int)", 1}, // no primary key
		{"create table", 1}, // bad syntax
		{"create table (id int ", 1}, // bad syntax
		{"create table people (id int primary key)", 0},
		{"create table people (id int primary key, age int)", 0},
		{"create table people (id int primary key, age int, first varchar(80), is_married bit)", 0},
		{"create table people (`id` int, `age` int, `first` varchar(80), `last` varchar(80), `title` varchar(80), `is_married` bit, primary key (`id`, `age`))", 0},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			working, err := dEnv.WorkingRoot()
			assert.Nil(t, err, "Unexpected error")
			assert.False(t, working.HasTable(tableName), "table exists before creating it")

			args := []string{"-q", test.query}
			commandStr := "dolt sql"
			result := Sql(commandStr, args, dEnv)
			assert.Equal(t, test.expectedRes, result)

			working, err = dEnv.WorkingRoot()
			assert.Nil(t, err, "Unexpected error")
			if test.expectedRes == 0 {
				assert.True(t, working.HasTable(tableName), "table doesn't exist after creating it")
			} else {
				assert.False(t, working.HasTable(tableName), "table shouldn't exist after error")
			}
		})
	}
}

func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(false)

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