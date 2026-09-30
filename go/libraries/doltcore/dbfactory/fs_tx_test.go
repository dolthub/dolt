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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestCreate_RoundTrip(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
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
			dest := "newdb"

			exists, _ := fs.Exists(dest)
			require.False(t, exists, "dest must not exist before create")

			tx, err := BeginCreate(fs, dest, uuid.Nil)
			require.NoError(t, err)

			tempExists, _ := fs.Exists(tx.TempPath())
			assert.True(t, tempExists, "temp directory must exist")
			assert.True(t, IsTempDir(fs, tx.TempPath()))

			destExists, _ := fs.Exists(dest)
			assert.False(t, destExists, "dest must not exist while uncommitted")

			require.NoError(t, tx.FS().MkDirs(DoltDir))
			payload := filepath.Join(DoltDir, "data.bin")
			require.NoError(t, tx.FS().WriteFile(payload, []byte("committed_payload"), 0o644))

			require.NoError(t, tx.Commit())

			destExists, _ = fs.Exists(dest)
			assert.True(t, destExists, "dest must exist after commit")
			tempExists, _ = fs.Exists(tx.TempPath())
			assert.False(t, tempExists, "temp dir must be cleaned")

			subFs, err := fs.WithWorkingDir(dest)
			require.NoError(t, err)
			hasData, _ := subFs.Exists(payload)
			assert.True(t, hasData, "payload must exist in dest")
		})
	}
}

func TestRollback(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	t.Run("entire directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		tx, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)

		require.NoError(t, tx.FS().WriteFile("uncommitted.txt", []byte("discard"), 0o644))
		require.NoError(t, tx.Rollback())

		exists, _ := parentFS.Exists("testdb")
		assert.False(t, exists, "dest must not exist on rollback")
		tempExists, _ := parentFS.Exists(tx.TempPath())
		assert.False(t, tempExists, "temp dir must be removed")
	})

	t.Run("preserved parent directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, parentFS.MkDirs("testdb"))
		userFile := filepath.Join("testdb", "keepme")
		require.NoError(t, parentFS.WriteFile(userFile, []byte("keep"), 0o644))

		tx, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(tx.TempPath(), "testdb"), "temp dir must be inside existing directory")

		require.NoError(t, tx.FS().MkDirs(DoltDir))
		require.NoError(t, tx.FS().WriteFile(filepath.Join(DoltDir, "uncommitted.bin"), []byte("drop"), 0o644))
		require.NoError(t, tx.Rollback())

		exists, _ := parentFS.Exists(userFile)
		assert.True(t, exists, "user files in directory must be preserved")
		doltExists, _ := parentFS.Exists(filepath.Join("testdb", DoltDir))
		assert.False(t, doltExists, ".dolt must not exist")
		tempExists, _ := parentFS.Exists(tx.TempPath())
		assert.False(t, tempExists, "temp dir must be removed")
	})
}

func TestCollision(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	t.Run("blocks completed database", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, parentFS.MkDirs("testdb"))
		subFs, err := parentFS.WithWorkingDir("testdb")
		require.NoError(t, err)
		require.NoError(t, subFs.MkDirs(DoltDir))

		_, err = BeginCreate(parentFS, "testdb", uuid.Nil)
		assert.ErrorIs(t, err, ErrExists)
	})

	t.Run("concurrent commit mutual exclusion", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		tx1, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)
		defer tx1.Rollback()
		require.NoError(t, tx1.FS().MkDirs(DoltDir))
		tx2, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)
		defer tx2.Rollback()

		require.NotEqual(t, tx1.TempPath(), tx2.TempPath())

		require.NoError(t, tx1.FS().WriteFile("proc1.txt", []byte("data1"), 0o644))
		require.NoError(t, tx2.FS().WriteFile("proc2.txt", []byte("data2"), 0o644))

		exists1, _ := tx2.FS().Exists("proc1.txt")
		assert.False(t, exists1, "tx2 must not see tx1 writes")
		exists2, _ := tx1.FS().Exists("proc2.txt")
		assert.False(t, exists2, "tx1 must not see tx2 writes")

		require.NoError(t, tx1.Commit())

		err = tx2.Commit()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrExists)

		require.NoError(t, tx2.Rollback())

		subFs, err := parentFS.WithWorkingDir("testdb")
		require.NoError(t, err)
		hasData1, _ := subFs.Exists("proc1.txt")
		assert.True(t, hasData1, "committed data must remain intact")
		hasData2, _ := subFs.Exists("proc2.txt")
		assert.False(t, hasData2, "rolled back data must not exist")

		_, err = BeginCreate(parentFS, "testdb", uuid.Nil)
		assert.ErrorIs(t, err, ErrExists)
	})

	t.Run("concurrent commit mutual exclusion with existing directory", func(t *testing.T) {
		parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
		require.NoError(t, err)

		require.NoError(t, parentFS.MkDirs("testdb"))

		tx1, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)
		defer tx1.Rollback()
		assert.True(t, tx1.DestPathExists())

		tx2, err := BeginCreate(parentFS, "testdb", uuid.Nil)
		require.NoError(t, err)
		defer tx2.Rollback()
		assert.True(t, tx2.DestPathExists())

		require.NoError(t, tx1.FS().MkDirs(DoltDir))
		require.NoError(t, tx2.FS().MkDirs(DoltDir))

		payload1 := filepath.Join(DoltDir, "proc1.txt")
		payload2 := filepath.Join(DoltDir, "proc2.txt")
		require.NoError(t, tx1.FS().WriteFile(payload1, []byte("data1"), 0o644))
		require.NoError(t, tx2.FS().WriteFile(payload2, []byte("data2"), 0o644))

		require.NoError(t, tx1.Commit())

		err = tx2.Commit()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrExists)

		subFs, err := parentFS.WithWorkingDir("testdb")
		require.NoError(t, err)
		hasData1, _ := subFs.Exists(payload1)
		assert.True(t, hasData1, "committed data must remain intact")
		hasData2, _ := subFs.Exists(payload2)
		assert.False(t, hasData2, "losing commit must not overwrite winner")
	})
}

func TestCommit_FailurePreservesTempDirUntilRollback(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	tx, err := BeginCreate(parentFS, "testdb", uuid.Nil)
	require.NoError(t, err)

	require.NoError(t, tx.FS().WriteFile("payload.txt", []byte("payload"), 0o644))
	require.NoError(t, parentFS.WriteFile("testdb", []byte("collision"), 0o644))

	err = tx.Commit()
	assert.Error(t, err)

	tempExists, _ := parentFS.Exists(tx.TempPath())
	assert.True(t, tempExists, "temp dir must be preserved on commit failure")

	err = tx.Rollback()
	assert.NoError(t, err)
	tempExists, _ = parentFS.Exists(tx.TempPath())
	assert.False(t, tempExists, "temp dir must be purged on rollback")
}

func TestFSCreateTx_UUID(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	parentFS, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	// Test with explicit identifier injection via uuid.NewV7.
	uExpected, err := uuid.NewV7()
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, uExpected)

	tx, err := BeginCreate(parentFS, "testdb", uExpected)
	require.NoError(t, err)
	defer func() { assert.NoError(t, tx.Rollback()) }()

	assert.Equal(t, uExpected, tx.UUID())
}

func TestFSCreateTx_CommitNonEmptyDestination(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	parentDir := t.TempDir()
	fs, err := filesys.LocalFilesysWithWorkingDir(parentDir)
	require.NoError(t, err)

	destDir := filepath.Join(parentDir, "existingdb")
	require.NoError(t, os.Mkdir(destDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(destDir, "somefile.txt"), []byte("data"), 0644))

	u, err := uuid.NewV7()
	require.NoError(t, err)

	tx := NewFSCreateTxForRecovery(fs, ".tmp-dolt-test-123", "existingdb", false, u)
	require.NoError(t, fs.MkDirs(".tmp-dolt-test-123"))
	defer func() { assert.NoError(t, tx.Rollback()) }()

	err = tx.Commit()
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExists))
}

func TestFSCreateTx_Recovery(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs := filesys.EmptyInMemFS("/")
	u, err := uuid.NewV7()
	require.NoError(t, err)

	tx := NewFSCreateTxForRecovery(fs, ".tmp-dolt-test-123", "test", false, u)
	require.NotNil(t, tx)
	assert.Equal(t, u, tx.UUID())
	assert.Equal(t, ".tmp-dolt-test-123", tx.TempPath())
	assert.False(t, tx.DestPathExists())
	assert.Nil(t, tx.FS())
}
