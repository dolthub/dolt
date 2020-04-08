package dolt

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
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

func TestDoltHarness(t *testing.T) {
	t.Run("should execute simple query against dolt", func(t *testing.T) {
		h := &DoltHarness{}
		home := "/home"
		wd := filepath.Join(home, "/harnesstester")
		fs := filesys.NewInMemFS([]string{}, nil, home)
		dEnv := createTestEnvWithFS(fs, wd)

		err := innerInit(h, dEnv)
		assert.Equal(t, nil, err)

		ct := "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);"
		i1 := "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);"
		i2 := "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);"

		q1 := "SELECT a,c,e FROM t1;"
		q2 := "SELECT b,d FROM t1;"
		q3 := "SELECT * FROM t1 WHERE d < 107;"
		q4 := "SELECT * FROM t1 WHERE d > 102;"

		err = executeStatement(h, ct)
		assert.Equal(t, nil, err)

		err = executeStatement(h, i1)
		assert.Equal(t, nil, err)

		err = executeStatement(h, i2)
		assert.Equal(t, nil, err)

		schema, results, err := executeQuery(h, q1)
		assert.Equal(t, nil, err)
		assert.Equal(t, schema, "III")
		assert.Equal(t, []string{"104", "102", "103", "107", "106", "109"}, results)

		schema, results, err = executeQuery(h, q2)
		assert.Equal(t, nil, err)
		assert.Equal(t, schema, "II")
		assert.Equal(t, []string{"100", "101", "105", "108"}, results)

		schema, results, err = executeQuery(h, q3)
		assert.Equal(t, nil, err)
		assert.Equal(t, "IIIII", schema)
		assert.Equal(t, []string{"104", "100", "102", "101", "103"}, results)

		schema, results, err = executeQuery(h, q4)
		assert.Equal(t, nil, err)
		assert.Equal(t, "IIIII", schema)
		assert.Equal(t, []string{"107", "105", "106", "108", "109"}, results)
	})
}
