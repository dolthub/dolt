package commands

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, filepath.Join(workingDir, dbfactory.DoltDir)}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func createUninitializedEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func TestCommandsRequireInitializedDir(t *testing.T) {
	tests := []struct {
		cmdStr   string
		args     []string
		commFunc cli.CommandFunc
	}{
		{"dolt config", []string{"-local", "-list"}, Config},
	}

	dEnv := createUninitializedEnv()
	for _, test := range tests {
		test.commFunc(test.cmdStr, test.args, dEnv)
	}
}
