// Copyright 2019 Dolthub, Inc.
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

package dtestutils

import (
	"context"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	TestHomeDirPrefix = "/user/dolt/"
	WorkingDirPrefix  = "/user/dolt/datasets/"
)

// CreateTestEnv creates a new DoltEnv suitable for testing. The CreateTestEnvWithName
// function should generally be preferred over this method, especially when working
// with tests using multiple databases within a MultiRepoEnv.
func CreateTestEnv() *env.DoltEnv {
	return CreateTestEnvWithName("test")
}

// CreateTestEnvForLocalFilesystem creates a new DoltEnv for testing, using a local FS, instead of an in-memory
// filesystem, for persisting files. This is useful for tests that require a disk-based filesystem and will not
// work correctly with an in-memory filesystem and in-memory blob store (e.g. dolt_undrop() tests).
func CreateTestEnvForLocalFilesystem(tempDir string) *env.DoltEnv {
	fs, err := filesys.LocalFilesysWithWorkingDir(tempDir)
	if err != nil {
		panic(err)
	}

	err = fs.MkDirs("test")
	if err != nil {
		panic(err)
	}

	fs, err = fs.WithWorkingDir("test")
	if err != nil {
		panic(err)
	}

	homeDir := filepath.Join(tempDir, "home")
	err = fs.MkDirs("home")
	if err != nil {
		panic(err)
	}
	homeDirFunc := func() (string, error) { return homeDir, nil }

	return createTestEnvWithNameAndFilesystem("test", fs, homeDirFunc)
}

// CreateTestEnvWithName creates a new DoltEnv suitable for testing and uses
// the specified name to distinguish it from other test envs. This function
// should generally be preferred over CreateTestEnv, especially when working with
// tests using multiple databases within a MultiRepoEnv.
func CreateTestEnvWithName(envName string) *env.DoltEnv {
	initialDirs := []string{TestHomeDirPrefix + envName, WorkingDirPrefix + envName}
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDirPrefix+envName)
	homeDirFunc := func() (string, error) { return TestHomeDirPrefix + envName, nil }

	return createTestEnvWithNameAndFilesystem(envName, fs, homeDirFunc)
}

// createTestEnvWithNameAndFilesystem creates a Dolt environment for testing, using the |envName| for the name, the
// specified |fs| for persisting files, and |homeDirFunc| for finding the location to load global Dolt configuration.
func createTestEnvWithNameAndFilesystem(envName string, fs filesys.Filesys, homeDirFunc func() (string, error)) *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"

	var urlStr string
	_, isInMemFs := fs.(*filesys.InMemFS)
	if isInMemFs {
		urlStr = doltdb.InMemDoltDB + envName
	} else {
		urlStr = doltdb.LocalDirDoltDB
	}

	dEnv := env.Load(context.Background(), homeDirFunc, fs, urlStr, "test")
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		config.UserNameKey:  name,
		config.UserEmailKey: email,
	})

	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch)
	if err != nil {
		panic("Failed to initialize environment:" + err.Error())
	}

	return dEnv
}
