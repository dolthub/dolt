// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/test"
	"github.com/dolthub/dolt/go/store/types"
)

func TestDirToDBName(t *testing.T) {
	replaceHyphenTests := map[string]string{
		"corona-virus":       "corona_virus",
		"  real - name     ": "real_name",
	}

	err := os.Setenv(dconfig.EnvDbNameReplace, "true")
	require.NoError(t, err)

	for dirName, expected := range replaceHyphenTests {
		actual := dbfactory.DirToDBName(dirName)
		assert.Equal(t, expected, actual)
	}

	allowHyphenTests := map[string]string{
		"irs":                "irs",
		"corona-virus":       "corona-virus",
		"  fake - name     ": "  fake - name     ",
	}

	err = os.Setenv(dconfig.EnvDbNameReplace, "")
	require.NoError(t, err)

	for dirName, expected := range allowHyphenTests {
		actual := dbfactory.DirToDBName(dirName)
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
		config.UserNameKey:  name,
		config.UserEmailKey: email,
	})

	err = dEnv.InitRepo(context.Background(), types.Format_Default, name, email, DefaultInitBranch)
	require.NoError(t, err)

	return Load(context.Background(), hdp, fs, urlStr, "test")
}

func TestMultiEnvForDirectory(t *testing.T) {
	rootPath, err := test.ChangeToTestDir(t.TempDir(), "TestDoltEnvAsMultiEnv")
	require.NoError(t, err)

	hdp := func() (string, error) { return rootPath, nil }
	envPath := filepath.Join(rootPath, " test---name _ 123")
	dEnv := initRepoWithRelativePath(t, envPath, hdp)

	mrEnv, err := MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	require.NoError(t, err)
	assert.Len(t, mrEnv.envs, 1)

	type envCmp struct {
		name    string
		doltDir string
	}

	expected := []envCmp{
		{
			name:    " test---name _ 123",
			doltDir: dEnv.GetDoltDir(),
		},
	}

	var actual []envCmp
	for _, env := range mrEnv.envs {
		actual = append(actual, envCmp{
			name:    env.name,
			doltDir: env.env.GetDoltDir(),
		})
	}

	assert.Equal(t, expected, actual)
}

func TestMultiEnvForDirectoryWithMultipleRepos(t *testing.T) {
	rootPath, err := test.ChangeToTestDir(t.TempDir(), "TestDoltEnvAsMultiEnvWithMultipleRepos")
	require.NoError(t, err)

	hdp := func() (string, error) { return rootPath, nil }
	envPath := filepath.Join(rootPath, " test---name _ 123")
	dEnv := initRepoWithRelativePath(t, envPath, hdp)
	subEnv1 := initRepoWithRelativePath(t, filepath.Join(envPath, "abc"), hdp)
	subEnv2 := initRepoWithRelativePath(t, filepath.Join(envPath, "def"), hdp)

	mrEnv, err := MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	require.NoError(t, err)
	assert.Len(t, mrEnv.envs, 3)

	expected := make(map[string]string)
	expected[" test---name _ 123"] = dEnv.GetDoltDir()
	expected["abc"] = subEnv1.GetDoltDir()
	expected["def"] = subEnv2.GetDoltDir()

	actual := make(map[string]string)
	for _, env := range mrEnv.envs {
		actual[env.name] = env.env.GetDoltDir()
	}

	assert.Equal(t, expected, actual)
}

func initMultiEnv(t *testing.T, testName string, names []string) (string, HomeDirProvider, map[string]*DoltEnv) {
	rootPath, err := test.ChangeToTestDir(t.TempDir(), testName)
	require.NoError(t, err)

	hdp := func() (string, error) { return rootPath, nil }

	envs := make(map[string]*DoltEnv)
	for _, name := range names {
		envPath := filepath.Join(rootPath, name)
		envs[name] = initRepoWithRelativePath(t, envPath, hdp)
	}

	return rootPath, hdp, envs
}
