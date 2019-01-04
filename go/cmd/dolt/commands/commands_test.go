package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
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
	initialDirs := []string{testHomeDir, filepath.Join(workingDir, env.DoltDir)}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(testHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func createUninitializedEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(testHomeDirFunc, fs, doltdb.InMemDoltDB)

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
