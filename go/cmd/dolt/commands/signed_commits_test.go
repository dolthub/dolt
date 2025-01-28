// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/gpg"

	"github.com/stretchr/testify/require"
)

const keyId = "573DA8C6366D04E35CDB1A44E09A0B208F666373"

func importKey(t *testing.T, ctx context.Context) {
	err := gpg.ImportKey(ctx, "testdata/signed_commits/private.pgp")
	require.NoError(t, err)

	ok, err := gpg.HasKey(ctx, keyId)
	require.NoError(t, err)
	require.True(t, ok)
}

func setupTestDB(t *testing.T, ctx context.Context, fs filesys.Filesys) string {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	err := filesys.CopyDir("testdata/signed_commits/db/", dbDir, fs)
	require.NoError(t, err)

	log.Println(dbDir)
	return dbDir
}

func TestSignAndVerifyCommit(t *testing.T) {
	tests := []struct {
		name       string
		localCfg   map[string]string
		commitArgs []string
		expectErr  bool
	}{
		{
			name:       "sign commit with command line key id",
			localCfg:   map[string]string{},
			commitArgs: []string{"-S", keyId, "-m", "test commit"},
			expectErr:  false,
		},
		{
			name:       "sign no key id, no keyid in config",
			localCfg:   map[string]string{},
			commitArgs: []string{"-S", "-m", "test commit"},
			expectErr:  true,
		},
	}

	ctx := context.Background()
	importKey(t, ctx)
	dbDir := setupTestDB(t, ctx, filesys.LocalFS)

	global := map[string]string{
		"user.name":  "First Last",
		"user.email": "test@dolthub.com",
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			apr, err := cli.CreateCommitArgParser().Parse(test.commitArgs)
			require.NoError(t, err)

			_, err = execCommand(ctx, t, dbDir, CommitCmd{}, test.commitArgs, apr, map[string]string{}, global)

			if test.expectErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			args := []string{"--show-signature"}
			apr, err = cli.CreateLogArgParser(false).Parse(args)
			require.NoError(t, err)

			logOutput, err := execCommand(ctx, t, dbDir, LogCmd{}, args, apr, map[string]string{}, global)
			require.NoError(t, err)
			require.Contains(t, logOutput, "Good signature from \"Test User <test@dolthub.com>\"")
		})
	}
}

func execCommand(ctx context.Context, t *testing.T, wd string, cmd cli.Command, args []string, apr *argparser.ArgParseResults, local, global map[string]string) (output string, err error) {
	err = os.Chdir(wd)
	if err != nil {
		err = fmt.Errorf("error changing directory to %s: %w", wd, err)
		return
	}

	var fs filesys.Filesys
	fs, err = filesys.LocalFilesysWithWorkingDir(wd)
	if err != nil {
		err = fmt.Errorf("error creating local filesystem with working directory %s: %w", wd, err)
		return
	}

	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, ".", "test")

	ch := config.NewConfigHierarchy()
	if global != nil {
		ch.AddConfig("global", config.NewMapConfig(global))
	}

	if local != nil {
		ch.AddConfig("local", config.NewMapConfig(local))
	}

	cfg := env.NewTestDoltCliConfigFromHierarchy(ch, fs)

	mr, err := env.MultiEnvForDirectory(ctx, ch, fs, dEnv.Version, dEnv)
	if err != nil {
		err = fmt.Errorf("error creating multi repo: %w", err)
		return
	}
	t.Cleanup(func() {
		mr.Iter(func(_ string, env *env.DoltEnv) (bool, error) {
			env.DoltDB.Close()
			return false, nil
		})
	})

	latebind, verr := BuildSqlEngineQueryist(ctx, dEnv.FS, mr, &cli.UserPassword{}, apr)
	if verr != nil {
		err = fmt.Errorf("error building sql engine: %w", verr)
		return
	}

	cliCtx, err := cli.NewCliContext(apr, cfg, dEnv.FS, latebind)
	if err != nil {
		err = fmt.Errorf("error creating cli context: %w", err)
		return
	}

	initialOut := os.Stdout
	initialErr := os.Stderr
	f, err := os.CreateTemp(t.TempDir(), "signed-commit-test-*")
	if err != nil {
		err = fmt.Errorf("error creating temp file: %w", err)
		return
	}

	os.Stdout = f
	os.Stderr = f
	restoreIO := cli.InitIO()

	defer func() {
		closeErr := f.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("error closing temp file: %w", closeErr)
		}

		restoreIO()

		os.Stdout = initialOut
		os.Stderr = initialErr

		outputBytes, readOutputErr := os.ReadFile(f.Name())
		if readOutputErr != nil && err == nil {
			err = fmt.Errorf("error reading temp file: %w", readOutputErr)
			return
		}

		output = string(outputBytes)
	}()

	n := cmd.Exec(ctx, cmd.Name(), args, dEnv, cliCtx)
	if n != 0 {
		err = fmt.Errorf("command %s %s exited with code %d", cmd.Name(), strings.Join(args, " "), n)
		return
	}

	return
}
