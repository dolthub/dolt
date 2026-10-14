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

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestLocalCSCacheKeepsRepositoriesInRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	require.NoError(t, os.Mkdir(root, 0755))
	fs, err := filesys.LocalFilesysWithWorkingDir(root)
	require.NoError(t, err)
	cache := NewLocalCSCache(fs)

	for _, repoPath := range []string{"../outside", "org/../../outside", filepath.Join(parent, "absolute")} {
		_, err := cache.Get(context.Background(), repoPath, types.Format_DOLT.VersionString())
		require.Error(t, err, repoPath)
	}
	_, err = os.Stat(filepath.Join(parent, "outside"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(parent, "absolute"))
	require.True(t, os.IsNotExist(err))

	outside := filepath.Join(parent, "target")
	require.NoError(t, os.Mkdir(outside, 0755))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	_, err = cache.Get(context.Background(), "link/repo", types.Format_DOLT.VersionString())
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(outside, "repo"))
	require.True(t, os.IsNotExist(err))

	store, err := cache.Get(context.Background(), "org/repo", types.Format_DOLT.VersionString())
	require.NoError(t, err)
	require.NoError(t, store.Close())
	_, err = os.Stat(filepath.Join(root, "org", "repo"))
	require.NoError(t, err)
}
