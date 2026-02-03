package blobstore

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_RefMissingIsNotFound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, "refs/dolt/data")
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.False(t, ok)

	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_ExistsAndGet_AllRange(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	want := []byte("hello manifest\n")
	commit, err := repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"manifest": want,
		"dir/file": []byte("abc"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, "refs/dolt/data")
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Exists(ctx, "missing")
	require.NoError(t, err)
	require.False(t, ok)

	// Validate key normalization: backslash -> slash.
	ok, err = bs.Exists(ctx, "dir\\file")
	require.NoError(t, err)
	require.True(t, ok)

	got, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, want, got)

	// Validate size + version on Get.
	rc, sz, ver2, err := bs.Get(ctx, "manifest", NewBlobRange(0, 5))
	require.NoError(t, err)
	require.Equal(t, uint64(len(want)), sz)
	require.Equal(t, commit, ver2)
	_ = rc.Close()
}

func TestGitBlobstore_Get_NotFoundMissingKey(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	_, err = repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"present": []byte("x"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, "refs/dolt/data")
	require.NoError(t, err)

	_, _, err = GetBytes(ctx, bs, "missing", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_BlobRangeSemantics(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	maxValue := int64(16 * 1024)
	testData := rangeData(0, maxValue)

	commit, err := repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"range": testData,
	}, "range fixture")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, "refs/dolt/data")
	require.NoError(t, err)

	// full range
	got, ver, err := GetBytes(ctx, bs, "range", AllRange)
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, rangeData(0, maxValue), got)

	// first 2048 bytes (1024 shorts)
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(0, 2048))
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, rangeData(0, 1024), got)

	// bytes 2048..4096 of original
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(2*1024, 2*1024))
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, rangeData(1024, 2048), got)

	// last 2048 bytes
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(-2*1024, 0))
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, rangeData(maxValue-1024, maxValue), got)

	// tail slice: beginning 2048 bytes from end, size 512
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(-2*1024, 512))
	require.NoError(t, err)
	require.Equal(t, commit, ver)
	require.Equal(t, rangeData(maxValue-1024, maxValue-768), got)
}

func TestGitBlobstore_InvalidKeysError(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	_, err = repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{"ok": []byte("x")}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, "refs/dolt/data")
	require.NoError(t, err)

	invalid := []string{
		"",
		"/abs",
		"../x",
		"a/../b",
		"a//b",
		"a/",
		".",
		"..",
		"a/./b",
		"a/\x00/b",
	}

	for _, k := range invalid {
		_, err := bs.Exists(ctx, k)
		require.Error(t, err, "expected error for key %q", k)

		_, _, _, err = bs.Get(ctx, k, AllRange)
		require.Error(t, err, "expected error for key %q", k)
	}
}

