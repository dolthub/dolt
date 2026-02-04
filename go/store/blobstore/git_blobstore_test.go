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

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func requireGitOnPath(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
}

func testIdentity() *git.Identity {
	return &git.Identity{Name: "gitblobstore test", Email: "gitblobstore@test.invalid"}
}

func TestGitBlobstore_RefMissingIsNotFound(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.False(t, ok)

	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))

	// For non-manifest keys, missing the ref is a hard error.
	_, _, _, err = bs.Get(ctx, "table", AllRange)
	require.Error(t, err)
	require.False(t, IsNotFoundError(err))
	var rnf *git.RefNotFoundError
	require.True(t, errors.As(err, &rnf))
}

func TestGitBlobstore_ExistsAndGet_AllRange(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	want := []byte("hello manifest\n")
	commit, err := repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": want,
		"dir/file": []byte("abc"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
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
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	_, err = repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"present": []byte("x"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
	require.NoError(t, err)

	_, _, err = GetBytes(ctx, bs, "missing", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_BlobRangeSemantics(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	maxValue := int64(16 * 1024)
	testData := rangeData(0, maxValue)

	commit, err := repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"range": testData,
	}, "range fixture")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
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
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	_, err = repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"ok": []byte("x")}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
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

		_, err = bs.Put(ctx, k, 1, bytes.NewReader([]byte("x")))
		require.Error(t, err, "expected error for key %q", k)
	}
}

func TestGitBlobstore_Put_RoundTripAndVersion(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(repo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	want := []byte("hello put\n")
	ver, err := PutBytes(ctx, bs, "k", want)
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	ok, err := bs.Exists(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok)

	got, ver2, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, want, got)
}

func TestGitBlobstore_Put_Overwrite(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(repo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	ver1, err := PutBytes(ctx, bs, "k", []byte("v1\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver1)

	ver2, err := PutBytes(ctx, bs, "k", []byte("v2\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver2)
	require.NotEqual(t, ver1, ver2)

	got, ver3, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver2, ver3)
	require.Equal(t, []byte("v2\n"), got)
}

type hookGitAPI struct {
	git.GitAPI

	ref string
	// if set, called once before the first UpdateRefCAS executes.
	onFirstCAS func(ctx context.Context, old git.OID)
	did        atomic.Bool
}

func (h *hookGitAPI) UpdateRefCAS(ctx context.Context, ref string, newOID git.OID, oldOID git.OID, msg string) error {
	if h.onFirstCAS != nil && !h.did.Swap(true) && ref == h.ref {
		h.onFirstCAS(ctx, oldOID)
	}
	return h.GitAPI.UpdateRefCAS(ctx, ref, newOID, oldOID, msg)
}

func writeKeyToRef(ctx context.Context, api git.GitAPI, ref string, key string, data []byte, author *git.Identity) (git.OID, error) {
	parent, ok, err := api.TryResolveRefCommit(ctx, ref)
	if err != nil {
		return "", err
	}

	indexDir, err := os.MkdirTemp("", "gitblobstore-test-index-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(indexDir) }()
	indexFile := filepath.Join(indexDir, "index")

	if ok {
		if err := api.ReadTree(ctx, parent, indexFile); err != nil {
			return "", err
		}
	} else {
		if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
			return "", err
		}
	}

	blobOID, err := api.HashObject(ctx, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", blobOID, key); err != nil {
		return "", err
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		return "", err
	}

	var parentPtr *git.OID
	if ok && parent != "" {
		p := parent
		parentPtr = &p
	}

	msg := "test external writer"
	commitOID, err := api.CommitTree(ctx, treeOID, parentPtr, msg, author)
	if err != nil {
		return "", err
	}

	if err := api.UpdateRef(ctx, ref, commitOID, msg); err != nil {
		return "", err
	}
	return commitOID, nil
}

func TestGitBlobstore_Put_ContentionRetryPreservesOtherKey(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	// Seed the ref so Put takes the CAS path.
	_, err = repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"base": []byte("base\n"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(repo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	origAPI := bs.api
	h := &hookGitAPI{GitAPI: origAPI, ref: DoltDataRef}
	h.onFirstCAS = func(ctx context.Context, old git.OID) {
		// Advance the ref to simulate another writer committing concurrently.
		_, _ = writeKeyToRef(ctx, origAPI, DoltDataRef, "external", []byte("external\n"), testIdentity())
	}
	bs.api = h

	ver, err := PutBytes(ctx, bs, "k", []byte("mine\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	got, ver2, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, []byte("mine\n"), got)

	got, _, err = GetBytes(ctx, bs, "external", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("external\n"), got)

	got, _, err = GetBytes(ctx, bs, "base", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("base\n"), got)

	// Sanity: BlobReader path still works for the new commit.
	rc, _, _, err := bs.Get(ctx, "k", AllRange)
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()
}
