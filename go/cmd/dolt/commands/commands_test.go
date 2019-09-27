// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
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
		test.commFunc(context.Background(), test.cmdStr, test.args, dEnv)
	}
}
