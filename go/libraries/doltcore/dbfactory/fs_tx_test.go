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

package dbfactory

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestInProgressMarkerRoundTrip(t *testing.T) {
	local, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	cases := []struct {
		name string
		fs   filesys.Filesys
	}{
		{"local", local},
		{"inmem", filesys.EmptyInMemFS("/")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := tc.fs
			marked, _ := fs.Exists(SafeToIgnoreMarkerFile)
			require.False(t, marked, "a fresh directory is not in progress")

			tx, err := BeginCreate(fs, ".")
			require.NoError(t, err)
			marked, _ = fs.Exists(SafeToIgnoreMarkerFile)
			assert.True(t, marked, "directory is in progress once marked")

			require.NoError(t, tx.Commit())
			marked, _ = fs.Exists(SafeToIgnoreMarkerFile)
			assert.False(t, marked, "directory is complete once the lock is committed")

			require.NoError(t, removeMarker(fs))
		})
	}
}

func TestRollback(t *testing.T) {
	t.Run("entire directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		tx, err := BeginCreate(parentFS, "testdb")
		require.NoError(t, err)

		require.NoError(t, tx.Rollback())
		exists, _ := parentFS.Exists("testdb")
		assert.False(t, exists, "entire directory should be removed on rollback")
	})

	t.Run("preserved parent directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, parentFS.MkDirs("testdb"))
		require.NoError(t, parentFS.WriteFile(filepath.Join("testdb", "keepme"), []byte("keep"), 0o644))

		tx, err := BeginCreate(parentFS, "testdb")
		require.NoError(t, err)
		subFs, err := parentFS.WithWorkingDir("testdb")
		require.NoError(t, err)
		require.NoError(t, subFs.MkDirs(DoltDir))

		require.NoError(t, tx.Rollback())
		exists, _ := parentFS.Exists(filepath.Join("testdb", "keepme"))
		assert.True(t, exists, "user files in directory should be preserved")
		exists, _ = parentFS.Exists(filepath.Join("testdb", DoltDir))
		assert.False(t, exists, ".dolt directory should be removed")
		marked, _ := subFs.Exists(SafeToIgnoreMarkerFile)
		assert.False(t, marked, "marker should be removed")
	})
}

func TestReclaimIncomplete(t *testing.T) {
	t.Run("reclaims inactive incomplete directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, parentFS.MkDirs("testdb"))
		subFs, err := parentFS.WithWorkingDir("testdb")
		require.NoError(t, err)
		require.NoError(t, subFs.WriteFile(SafeToIgnoreMarkerFile, nil, safeToIgnoreMarkerPerm))

		tx, err := BeginCreate(parentFS, "testdb")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		marked, _ := subFs.Exists(SafeToIgnoreMarkerFile)
		assert.False(t, marked, "reclaimed directory is complete after commit")
	})

	t.Run("blocks actively locked incomplete directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		tx1, err := BeginCreate(parentFS, "testdb")
		require.NoError(t, err)
		defer tx1.Rollback()

		_, err = BeginCreate(parentFS, "testdb")
		assert.ErrorIs(t, err, ErrLocked)
	})
}
