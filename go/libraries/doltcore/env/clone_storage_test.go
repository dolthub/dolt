// Copyright 2026 Dolthub, Inc.
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

package env_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestEnvForCloneTakesCopyParamsBeforeInit(t *testing.T) {
	ctx := context.Background()
	s := env.UseToyStorage(t)
	src, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer src.Close()

	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }
	var configured bool
	dEnv, err := actions.EnvForCloneOf(ctx, src, env.NoRemote, "cloned", fs, "test", hdp, func(dEnv *env.DoltEnv) {
		configured = true
		assert.False(t, dEnv.HasDoltDataDir(), "configure runs before the database is created")
		dEnv.AddDBLoadParams(map[string]string{"configured": "1"})
	})
	require.NoError(t, err)
	defer dEnv.Close()

	assert.True(t, configured)
	assert.Equal(t, []*doltdb.DoltDB{src}, s.Copied)
	require.NotEmpty(t, s.Loaded)
	assert.Equal(t, map[string]interface{}{"configured": "1", "toy_copy": "1"}, s.Loaded[0], "the database is created with the copy params")
}

func TestEnvForCloneRemovesItsStateWhenCopyParamsFail(t *testing.T) {
	ctx := context.Background()
	s := env.UseToyStorage(t)
	s.CopyErr = errors.New("toy")
	src, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer src.Close()

	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	// A git remote opened for the clone caches its repository in the directory first.
	cache := filepath.Join("cloned", dbfactory.DoltDir, dbfactory.GitRemoteCacheDirName)
	require.NoError(t, fs.MkDirs(cache))
	hdp := func() (string, error) { return fs.TempDir(), nil }
	_, err = actions.EnvForCloneOf(ctx, src, env.NoRemote, "cloned", fs, "test", hdp)
	require.ErrorIs(t, err, s.CopyErr)
	exists, _ := fs.Exists(filepath.Join("cloned", dbfactory.DoltDir))
	assert.False(t, exists, "the clone's state is removed")
}
