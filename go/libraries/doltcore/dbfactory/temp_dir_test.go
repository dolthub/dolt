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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestCreateTempDir_NewDir(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	dest := filepath.Join("parent", "newdb")
	require.NoError(t, fs.MkDirs("parent"))

	tempDir, _, err := CreateTempDir(fs, dest, false, uuid.Nil)
	require.NoError(t, err)

	assert.Equal(t, "parent", filepath.Dir(tempDir))
	base := filepath.Base(tempDir)
	assert.True(t, strings.HasPrefix(base, TempDirPrefix+"newdb-"), "name must start with .tmp-dolt-newdb-")

	exists, isDir := fs.Exists(tempDir)
	assert.True(t, exists && isDir, "temp directory must exist")
}

func TestCreateTempDir_ExistingDir(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	dest := "existing_dir"
	require.NoError(t, fs.MkDirs(dest))

	tempDir, _, err := CreateTempDir(fs, dest, true, uuid.Nil)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(tempDir, dest), "temp dir must be inside existing_dir")
	base := filepath.Base(tempDir)
	prefix := fmt.Sprintf("%s%d-", TempDirPrefix, os.Getpid())
	assert.True(t, strings.HasPrefix(base, prefix), "name must start with .tmp-dolt-<pid>-")

	exists, isDir := fs.Exists(tempDir)
	assert.True(t, exists && isDir, "temp directory must exist")
}

func TestIsTempDir(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs := filesys.EmptyInMemFS("/")
	tests := []struct {
		name     string
		expected bool
	}{
		{TempDirPrefix + "mydb-1234-uuid", true},
		{TempDirPrefix + "1234-uuid", true},
		{filepath.Join("sub", TempDirPrefix+"db"), true},
		{"mydb", false},
		{".dolt", false},
		{".doltcfg", false},
		{"tmp", false},
		{"", false},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, IsTempDir(fs, tc.name), "IsTempDir(%q) mismatch", tc.name)
	}

	t.Run("legacy in-progress marker", func(t *testing.T) {
		fs := filesys.EmptyInMemFS("/")
		require.NoError(t, fs.MkDirs("legacy_db"))
		marker := filepath.Join("legacy_db", SafeToIgnoreMarkerFile)
		require.NoError(t, fs.WriteFile(marker, nil, 0o644))

		assert.True(t, IsTempDir(fs, "legacy_db"))

		require.NoError(t, fs.Delete(marker, false))
		assert.False(t, IsTempDir(fs, "legacy_db"))
	})
}

func TestParseTempDirMeta(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	before := time.Now().Truncate(time.Millisecond)
	tempDir, u, err := CreateTempDir(fs, "testdb", false, uuid.Nil)
	require.NoError(t, err)
	after := time.Now().Add(time.Second)

	dbName, pid, parsedU, createdAt, ok := ParseTempDirMeta(tempDir)
	require.True(t, ok, "must successfully parse metadata")
	assert.Equal(t, "testdb", dbName)
	assert.Equal(t, os.Getpid(), pid, "pid must match current process")
	assert.Equal(t, u, parsedU)
	assert.True(t, !createdAt.Before(before) && !createdAt.After(after), "timestamp must be within range")

	// Test insideDest format with injected identifier
	uInjected, err := uuid.NewV7()
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, uInjected)
	tempDirInside, uInside, err := CreateTempDir(fs, "existing_dir", true, uInjected)
	require.NoError(t, err)
	assert.Equal(t, uInjected, uInside)
	dbNameInside, pidInside, parsedUInside, _, ok := ParseTempDirMeta(tempDirInside)
	require.True(t, ok)
	assert.Empty(t, dbNameInside, "insideDest temp dir has empty dbName")
	assert.Equal(t, os.Getpid(), pidInside)
	assert.Equal(t, uInside, parsedUInside)

	_, _, _, _, ok = ParseTempDirMeta("invalid")
	assert.False(t, ok)
	_, _, _, _, ok = ParseTempDirMeta(".tmp-dolt-notanumber-uuid")
	assert.False(t, ok)
}

func TestCleanupTempDirs_ContextCancellation(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = CleanupTempDirs(ctx, fs, ".", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestCleanupTempDirs_LiveProcessPreserved(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	tempDir, _, err := CreateTempDir(fs, "active_db", false, uuid.Nil)
	require.NoError(t, err)
	defer func() { _ = fs.Delete(tempDir, true) }()

	assert.False(t, IsTempDirStale(tempDir), "live process temp dir must not be stale")

	ctx := context.Background()
	require.NoError(t, CleanupTempDirs(ctx, fs, ".", IsTempDirStale))

	exists, _ := fs.Exists(tempDir)
	assert.True(t, exists, "active temp dir must survive cleanup")
}

func TestCleanupTempDirs_TerminatedProcessReclaimed(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	deadPID := 99999999
	u := "018f3a5b-7c8d-7e9f-a0b1-c2d3e4f5a6b7"
	deadDir := filepath.Join(".", fmt.Sprintf("%sdead_db-%d-%s", TempDirPrefix, deadPID, u))
	require.NoError(t, fs.MkDirs(deadDir))

	assert.True(t, IsTempDirStale(deadDir), "terminated process temp dir must be stale")

	ctx := context.Background()
	require.NoError(t, CleanupTempDirs(ctx, fs, ".", IsTempDirStale))

	exists, _ := fs.Exists(deadDir)
	assert.False(t, exists, "stale temp dir must be removed")
}
