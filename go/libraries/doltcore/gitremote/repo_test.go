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

package gitremote

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	fullArgs := args
	if dir != "" {
		fullArgs = append([]string{"-C", dir}, args...)
	}
	cmd := exec.Command("git", fullArgs...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s failed: %s", strings.Join(args, " "), string(out))
	return string(out)
}

// createBareRepo creates a bare git repository for testing.
func createBareRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bareDir := filepath.Join(dir, "bare.git")
	runGit(t, "", "init", "--bare", bareDir)

	return bareDir
}

func TestOpenOptions_Defaults(t *testing.T) {
	opts := OpenOptions{
		URL: "file:///test/repo",
	}

	assert.Equal(t, "file:///test/repo", opts.URL)
	assert.Empty(t, opts.Ref)
	assert.Empty(t, opts.LocalPath)
}

func TestGitRepo_OpenBareRepo(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	assert.Equal(t, bareDir, gr.URL())
	assert.Equal(t, DefaultRef, gr.Ref())
}

func TestGitRepo_WriteAndReadFile(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Write a file
	testContent := []byte("hello, world!")
	err = gr.WriteFile("test/file.txt", testContent)
	require.NoError(t, err)

	// Read it back
	content, err := gr.ReadFile("test/file.txt")
	require.NoError(t, err)
	assert.Equal(t, testContent, content)

	// Check existence
	exists, err := gr.FileExists("test/file.txt")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = gr.FileExists("nonexistent.txt")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestGitRepo_DeleteFile(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Write a file
	err = gr.WriteFile("todelete.txt", []byte("delete me"))
	require.NoError(t, err)

	// Verify it exists
	exists, err := gr.FileExists("todelete.txt")
	require.NoError(t, err)
	assert.True(t, exists)

	// Delete it
	err = gr.DeleteFile("todelete.txt")
	require.NoError(t, err)

	// Verify it's gone
	exists, err = gr.FileExists("todelete.txt")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestGitRepo_ListFiles(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Create some files
	err = gr.WriteFile("dir/file1.txt", []byte("1"))
	require.NoError(t, err)
	err = gr.WriteFile("dir/file2.txt", []byte("2"))
	require.NoError(t, err)
	err = gr.WriteFile("dir/file3.txt", []byte("3"))
	require.NoError(t, err)

	// List files
	files, err := gr.ListFiles("dir")
	require.NoError(t, err)
	assert.Len(t, files, 3)
	assert.Contains(t, files, "file1.txt")
	assert.Contains(t, files, "file2.txt")
	assert.Contains(t, files, "file3.txt")

	// List nonexistent directory
	files, err = gr.ListFiles("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, files)
}

func TestGitRepo_CommitAndPush(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Write a file
	err = gr.WriteFile("data.txt", []byte("some data"))
	require.NoError(t, err)

	// Commit
	hash, err := gr.Commit(ctx, "Add data file")
	require.NoError(t, err)
	assert.NotEmpty(t, hash)

	// Push
	err = gr.Push(ctx)
	require.NoError(t, err)

	// Verify the commit is on the custom ref
	currentHash, err := gr.CurrentCommit()
	require.NoError(t, err)
	assert.Equal(t, hash, currentHash)
}

func TestGitRepo_CommitNoChanges(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Try to commit with no changes
	_, err = gr.Commit(ctx, "Empty commit")
	assert.ErrorIs(t, err, ErrNothingToCommit)
}

func TestGitRepo_InitRemote(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	// Check not initialized
	initialized, err := gr.IsInitialized()
	require.NoError(t, err)
	assert.False(t, initialized)

	// Initialize
	err = gr.InitRemote(ctx)
	require.NoError(t, err)

	// Check initialized
	initialized, err = gr.IsInitialized()
	require.NoError(t, err)
	assert.True(t, initialized)

	// Verify README exists
	content, err := gr.ReadFile(DoltRemoteDir + "/README.md")
	require.NoError(t, err)
	assert.Contains(t, string(content), "Dolt Remote Data")

	// Verify data directory exists
	exists, err := gr.FileExists(DoltRemoteDataDir + "/.gitkeep")
	require.NoError(t, err)
	assert.True(t, exists)

	// Initialize again (should be idempotent)
	err = gr.InitRemote(ctx)
	require.NoError(t, err)
}

func TestGitRepo_CustomRef(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	workDir := filepath.Join(t.TempDir(), "work")
	customRef := "refs/dolt/custom"

	gr, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		Ref:       customRef,
		LocalPath: workDir,
	})
	require.NoError(t, err)
	defer gr.Close()

	assert.Equal(t, customRef, gr.Ref())

	// Write, commit, push
	err = gr.WriteFile("custom.txt", []byte("custom ref data"))
	require.NoError(t, err)

	_, err = gr.Commit(ctx, "Custom ref commit")
	require.NoError(t, err)

	err = gr.Push(ctx)
	require.NoError(t, err)

	// Verify the ref exists in the bare repo
	// git --git-dir <bare> show-ref --verify refs/dolt/custom
	out := runGit(t, "", fmt.Sprintf("--git-dir=%s", bareDir), "show-ref", "--verify", customRef)
	assert.NotEmpty(t, out)
}

func TestGitRepo_FetchUpdates(t *testing.T) {
	bareDir := createBareRepo(t)
	ctx := context.Background()

	// First client writes and pushes to custom ref
	workDir1 := filepath.Join(t.TempDir(), "work1")
	gr1, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir1,
	})
	require.NoError(t, err)
	defer gr1.Close()

	err = gr1.WriteFile("file1.txt", []byte("from client 1"))
	require.NoError(t, err)

	hash1, err := gr1.Commit(ctx, "Client 1 commit")
	require.NoError(t, err)

	err = gr1.Push(ctx)
	require.NoError(t, err)

	// Second client opens and should see the changes after fetch
	workDir2 := filepath.Join(t.TempDir(), "work2")
	gr2, err := Open(ctx, OpenOptions{
		URL:       bareDir,
		LocalPath: workDir2,
	})
	require.NoError(t, err)
	defer gr2.Close()

	// Checkout the ref to see the files
	err = gr2.CheckoutRef(ctx)
	require.NoError(t, err)

	// Should see the file from client 1
	content, err := gr2.ReadFile("file1.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("from client 1"), content)

	// Current commit should match
	hash2, err := gr2.CurrentCommit()
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)
}

func TestIsRefNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "reference not found message",
			err:  fmt.Errorf("reference not found"),
			want: true,
		},
		{
			name: "couldn't find remote ref message",
			err:  fmt.Errorf("couldn't find remote ref refs/dolt/data"),
			want: true,
		},
		{
			name: "other error",
			err:  assert.AnError,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRefNotFoundError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConstants(t *testing.T) {
	assert.Equal(t, "refs/dolt/data", DefaultRef)
	assert.Equal(t, "origin", DefaultRemoteName)
	assert.Equal(t, ".dolt_remote", DoltRemoteDir)
	assert.Equal(t, ".dolt_remote/data", DoltRemoteDataDir)
}
