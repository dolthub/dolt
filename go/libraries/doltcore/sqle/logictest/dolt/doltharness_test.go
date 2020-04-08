package dolt

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func createTestEnvWithFS(fs filesys.Filesys, workingDir string) *env.DoltEnv {
	testHomeDirFunc := func() (string, error) { return workingDir, nil }
	const name = "test mcgibbins"
	const email = "bigfakeytester@fake.horse"
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")
	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email)
	if err != nil {
		panic("Failed to initialize environment")
	}
	return dEnv
}

type statementTest struct {
	statement string
	expErr    error
}

type queryTest struct {
	query      string
	expErr     error
	expSchema  string
	expResults []string
}

func TestDoltHarness(t *testing.T) {
	home := "/home"
	wd := filepath.Join(home, "/harnesstester")

	statementTests := []statementTest{
		{
			statement: "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);",
			expErr:    nil,
		},
		{
			statement: "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);",
			expErr:    nil,
		},
		{
			statement: "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);",
			expErr:    nil,
		},
	}

	queryTests := []queryTest{
		{
			query:      "SELECT a,c,e FROM t1;",
			expErr:     nil,
			expSchema:  "III",
			expResults: []string{"104", "102", "103", "107", "106", "109"},
		},
		{
			query:      "SELECT b,d FROM t1;",
			expErr:     nil,
			expSchema:  "II",
			expResults: []string{"100", "101", "105", "108"},
		},
		{
			query:      "SELECT * FROM t1 WHERE d < 107;",
			expErr:     nil,
			expSchema:  "IIIII",
			expResults: []string{"104", "100", "102", "101", "103"},
		},
		{
			query:      "SELECT * FROM t1 WHERE d > 102;",
			expErr:     nil,
			expSchema:  "IIIII",
			expResults: []string{"107", "105", "106", "108", "109"},
		},
	}

	t.Run("should execute simple sql statements against Dolt", func(t *testing.T) {
		h := &DoltHarness{}
		fs := filesys.NewInMemFS([]string{}, nil, home)
		dEnv := createTestEnvWithFS(fs, wd)

		err := innerInit(h, dEnv)
		assert.Equal(t, nil, err)

		for _, test := range statementTests {
			err = executeStatement(h, test.statement)
			assert.Equal(t, test.expErr, err)
		}
	})

	t.Run("should execute simple sql queries against Dolt", func(t *testing.T) {
		h := &DoltHarness{}
		fs := filesys.NewInMemFS([]string{}, nil, home)
		dEnv := createTestEnvWithFS(fs, wd)

		err := innerInit(h, dEnv)
		assert.Equal(t, nil, err)

		// setup repo with statements
		for _, test := range statementTests {
			err = executeStatement(h, test.statement)
			assert.Equal(t, test.expErr, err)
		}

		// test queries
		for _, test := range queryTests {
			schema, results, err := executeQuery(h, test.query)
			assert.Equal(t, test.expErr, err)
			assert.Equal(t, test.expSchema, schema)
			assert.Equal(t, test.expResults, results)
		}
	})
}
