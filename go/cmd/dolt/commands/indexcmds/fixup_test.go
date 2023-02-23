package indexcmds

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"path/filepath"
	"testing"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, filepath.Join(workingDir, dbfactory.DoltDir), filepath.Join(workingDir, dbfactory.DoltDataDir)}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")

	return dEnv
}

func createUninitializedEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")

	return dEnv
}

func TestIndexFixup(t *testing.T) {
	tests := []struct {
		cmdStr string
		args   []string
		comm   cli.Command
	}{
		{"dolt config", []string{"-local", "-list"}, FixupCmd{}},
	}

	dEnv := createUninitializedEnv()
	for _, test := range tests {
		test.comm.Exec(context.Background(), test.cmdStr, test.args, dEnv)
	}
}
