// Copyright 2020 Liquidata, Inc.
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

package env

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/test"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestDirToDBName(t *testing.T) {
	tests := map[string]string{
		"irs":                "irs",
		"corona-virus":       "corona_virus",
		"  fake - name     ": "fake_name",
	}

	for dirName, expected := range tests {
		actual := dirToDBName(dirName)
		assert.Equal(t, expected, actual)
	}
}

func TestGetRepoRootDir(t *testing.T) {
	tests := []struct {
		path     string
		sep      string
		expected string
	}{
		{``, `/`, ``},
		{``, `\`, ``},
		{`/`, `/`, ``},
		{`C:\`, `\`, ``},
		{`.dolt/noms`, `/`, ``},
		{`.dolt\noms`, `\`, ``},
		{`name/.dolt/noms`, `/`, `name`},
		{`name\.dolt\noms`, `\`, `name`},
		{`name/.dolt/noms/`, `/`, `name`},
		{`name\.dolt\noms\`, `\`, `name`},
		{`/var/folders/w6/mhtq880n6y55xxm3_2kn0bs80000gn/T/dolt-repo-76581`, `/`, `dolt-repo-76581`},
		{`/var/folders/w6/mhtq880n6y55xxm3_2kn0bs80000gn/T/dolt-repo-76581/.dolt/noms`, `/`, `dolt-repo-76581`},
		{`/Users/u/name/.dolt/noms`, `/`, `name`},
		{`C:\files\name\.dolt\noms`, `\`, `name`},
		{`/Users/u/name/.dolt/noms/`, `/`, `name`},
		{`C:\files\name\.dolt\noms\`, `\`, `name`},
		{`//Users////u//name//.dolt/noms/////`, `/`, `name`},
		{`C:\\files\\\\name\\.dolt\noms\\\\\\`, `\`, `name`},
	}

	for _, test := range tests {
		actual := getRepoRootDir(test.path, test.sep)
		assert.Equal(t, test.expected, actual, "For '%s' expected '%s' got '%s'", test.path, test.expected, actual)
	}
}

func initRepoWithRelativePath(t *testing.T, envPath string, hdp HomeDirProvider) *DoltEnv {
	err := filesys.LocalFS.MkDirs(envPath)
	require.NoError(t, err)

	fs, err := filesys.LocalFilesysWithWorkingDir(envPath)
	require.NoError(t, err)

	urlStr := earl.FileUrlFromPath(filepath.Join(envPath, ".dolt", "noms"), os.PathSeparator)
	dEnv := Load(context.Background(), hdp, fs, urlStr, "test")
	cfg, _ := dEnv.Config.GetConfig(GlobalConfig)
	cfg.SetStrings(map[string]string{
		UserNameKey:  name,
		UserEmailKey: email,
	})

	err = dEnv.InitRepo(context.Background(), types.Format_7_18, name, email)
	require.NoError(t, err)

	return dEnv
}

func TestDoltEnvAsMultiEnv(t *testing.T) {
	rootPath, err := test.ChangeToTestDir("TestDoltEnvAsMultiEnv")
	require.NoError(t, err)

	hdp := func() (string, error) { return rootPath, nil }
	envPath := filepath.Join(rootPath, " test---name _ 123")
	dEnv := initRepoWithRelativePath(t, envPath, hdp)

	mrEnv := DoltEnvAsMultiEnv(dEnv)
	assert.Len(t, mrEnv, 1)

	for k, v := range mrEnv {
		assert.Equal(t, "test_name_123", k)
		assert.Equal(t, dEnv, v)
	}
}

func initMultiEnv(t *testing.T, testName string, names []string) (string, HomeDirProvider, map[string]*DoltEnv) {
	rootPath, err := test.ChangeToTestDir(testName)
	require.NoError(t, err)

	hdp := func() (string, error) { return rootPath, nil }

	envs := make(map[string]*DoltEnv)
	for _, name := range names {
		envPath := filepath.Join(rootPath, name)
		envs[name] = initRepoWithRelativePath(t, envPath, hdp)
	}

	return rootPath, hdp, envs
}

func TestLoadMultiEnv(t *testing.T) {
	names := []string{"env 1", " env  2", "env-3"}
	rootPath, hdp, _ := initMultiEnv(t, "TestLoadMultiEnv", names)

	envNamesAndPaths := make([]EnvNameAndPath, len(names))
	for i, name := range names {
		envNamesAndPaths[i] = EnvNameAndPath{name, filepath.Join(rootPath, name)}
	}

	mrEnv, err := LoadMultiEnv(context.Background(), hdp, filesys.LocalFS, "test", envNamesAndPaths...)
	require.NoError(t, err)

	for _, name := range names {
		_, ok := mrEnv[name]
		assert.True(t, ok)
	}
}

func TestLoadMultiEnvFromDir(t *testing.T) {
	dirNameToDBName := map[string]string{
		"env 1":   "env_1",
		" env  2": "env_2",
		"env-3":   "env_3",
	}

	names := make([]string, 0, len(dirNameToDBName))
	for k := range dirNameToDBName {
		names = append(names, k)
	}

	rootPath, hdp, envs := initMultiEnv(t, "TestLoadMultiEnvFromDir", names)
	mrEnv, err := LoadMultiEnvFromDir(context.Background(), hdp, filesys.LocalFS, rootPath, "test")
	require.NoError(t, err)

	assert.Len(t, mrEnv, len(names))
	for _, dirName := range names {
		dbName := dirNameToDBName[dirName]
		_, ok := envs[dirName]
		require.True(t, ok)
		_, ok = mrEnv[dbName]
		require.True(t, ok)
	}
}
