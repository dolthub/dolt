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

package blobstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/gitremote"
)

// createTestGitRepo creates a bare git repo and initializes it with a commit
// so it can be cloned.
func createTestGitRepo(t *testing.T) string {
	t.Helper()

	// Create bare repo
	bareDir := t.TempDir()
	barePath := filepath.Join(bareDir, "test.git")
	cmd := exec.Command("git", "init", "--bare", barePath)
	require.NoError(t, cmd.Run())

	// Create a working repo, add initial commit, push to bare
	workDir := t.TempDir()
	cmd = exec.Command("git", "init", workDir)
	require.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", workDir, "config", "user.email", "test@test.com")
	require.NoError(t, cmd.Run())
	cmd = exec.Command("git", "-C", workDir, "config", "user.name", "Test")
	require.NoError(t, cmd.Run())

	// Create .dolt_remote structure
	dataDir := filepath.Join(workDir, gitremote.DoltRemoteDataDir)
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	readmePath := filepath.Join(workDir, gitremote.DoltRemoteDir, "README.md")
	require.NoError(t, os.WriteFile(readmePath, []byte("# Dolt Remote\n"), 0644))

	cmd = exec.Command("git", "-C", workDir, "add", ".")
	require.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", workDir, "commit", "-m", "init")
	require.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", workDir, "remote", "add", "origin", barePath)
	require.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", workDir, "push", "-u", "origin", "HEAD:refs/dolt/data")
	require.NoError(t, cmd.Run())

	return barePath
}

func TestGitBlobstore_Path(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	assert.Equal(t, barePath, bs.Path())
}

func TestGitBlobstore_PutAndGet(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put a blob
	data := []byte("hello world")
	_, err = bs.Put(ctx, "test-key", int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)

	// Get it back
	rc, size, _, err := bs.Get(ctx, "test-key", BlobRange{})
	require.NoError(t, err)
	defer rc.Close()

	assert.Equal(t, uint64(len(data)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestGitBlobstore_Exists(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Should not exist initially
	exists, err := bs.Exists(ctx, "nonexistent")
	require.NoError(t, err)
	assert.False(t, exists)

	// Put a blob
	data := []byte("test data")
	_, err = bs.Put(ctx, "exists-key", int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)

	// Should exist now
	exists, err = bs.Exists(ctx, "exists-key")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestGitBlobstore_GetNotFound(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	_, _, _, err = bs.Get(ctx, "nonexistent", BlobRange{})
	assert.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_GetByteRange(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put a blob
	data := []byte("0123456789")
	_, err = bs.Put(ctx, "range-key", int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)

	// Get a range
	rc, _, _, err := bs.Get(ctx, "range-key", BlobRange{offset: 2, length: 5})
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("23456"), got)
}

func TestGitBlobstore_Concatenate(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put source blobs
	_, err = bs.Put(ctx, "src1", 5, bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	_, err = bs.Put(ctx, "src2", 5, bytes.NewReader([]byte("world")))
	require.NoError(t, err)

	// Concatenate
	_, err = bs.Concatenate(ctx, "combined", []string{"src1", "src2"})
	require.NoError(t, err)

	// Verify
	rc, _, _, err := bs.Get(ctx, "combined", BlobRange{})
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("helloworld"), got)
}

func TestGitBlobstore_CheckAndPut(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put initial blob
	data1 := []byte("initial")
	ver1, err := bs.Put(ctx, "cap-key", int64(len(data1)), bytes.NewReader(data1))
	require.NoError(t, err)

	// CheckAndPut with correct version should succeed
	data2 := []byte("updated")
	_, err = bs.CheckAndPut(ctx, ver1, "cap-key", int64(len(data2)), bytes.NewReader(data2))
	require.NoError(t, err)

	// Verify update
	rc, _, _, err := bs.Get(ctx, "cap-key", BlobRange{})
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, data2, got)
}

func TestGitBlobstore_CheckAndPut_VersionMismatch(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put initial blob
	data1 := []byte("initial")
	_, err = bs.Put(ctx, "cap-key2", int64(len(data1)), bytes.NewReader(data1))
	require.NoError(t, err)

	// CheckAndPut with wrong version should fail
	data2 := []byte("updated")
	_, err = bs.CheckAndPut(ctx, "wrong-version", "cap-key2", int64(len(data2)), bytes.NewReader(data2))
	assert.Error(t, err)

	var capErr CheckAndPutError
	assert.ErrorAs(t, err, &capErr)
}

func TestGitBlobstore_Sync(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put some blobs
	_, err = bs.Put(ctx, "sync1", 4, bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	_, err = bs.Put(ctx, "sync2", 4, bytes.NewReader([]byte("more")))
	require.NoError(t, err)

	// Sync to remote
	err = bs.Sync(ctx, "test sync")
	require.NoError(t, err)

	// Create a new blobstore from the same repo to verify data was pushed
	bs2, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs2.Close()

	// Verify blobs exist in the new instance
	exists, err := bs2.Exists(ctx, "sync1")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = bs2.Exists(ctx, "sync2")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestGitBlobstore_MultiplePuts(t *testing.T) {
	barePath := createTestGitRepo(t)
	ctx := context.Background()

	bs, err := NewGitBlobstore(ctx, barePath, gitremote.DefaultRef, "")
	require.NoError(t, err)
	defer bs.Close()

	// Put multiple blobs
	for i := 0; i < 10; i++ {
		key := "multi-" + string(rune('a'+i))
		data := []byte{byte(i)}
		_, err = bs.Put(ctx, key, 1, bytes.NewReader(data))
		require.NoError(t, err)
	}

	// Verify all exist
	for i := 0; i < 10; i++ {
		key := "multi-" + string(rune('a'+i))
		exists, err := bs.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists, "key %s should exist", key)
	}
}
