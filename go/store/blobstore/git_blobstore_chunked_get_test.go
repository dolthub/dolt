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
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_Get_ChunkedTree_AllAndRanges(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	part1 := []byte("abc")
	part2 := []byte("defgh")
	commitOID, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"chunked/0001": part1,
		"chunked/0002": part2,
	}, "seed chunked tree")
	require.NoError(t, err)

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(remoteRunner)
	treeOID, _, err := api.ResolvePathObject(ctx, git.OID(commitOID), "chunked")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	wantAll := append(append([]byte(nil), part1...), part2...)

	got, ver, err := GetBytes(ctx, bs, "chunked", AllRange)
	require.NoError(t, err)
	require.Equal(t, treeOID.String(), ver)
	require.Equal(t, wantAll, got)

	// Range spanning boundary: offset 2 length 4 => "cdef"
	got, ver, err = GetBytes(ctx, bs, "chunked", NewBlobRange(2, 4))
	require.NoError(t, err)
	require.Equal(t, treeOID.String(), ver)
	require.Equal(t, []byte("cdef"), got)

	// Tail read last 3 bytes => "fgh"
	got, ver, err = GetBytes(ctx, bs, "chunked", NewBlobRange(-3, 0))
	require.NoError(t, err)
	require.Equal(t, treeOID.String(), ver)
	require.Equal(t, []byte("fgh"), got)

	// Validate size returned is logical size.
	rc, sz, ver2, err := bs.Get(ctx, "chunked", NewBlobRange(0, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(len(wantAll)), sz)
	require.Equal(t, treeOID.String(), ver2)
	_ = rc.Close()
}

func TestGitBlobstore_Get_ChunkedTree_InvalidPartsError(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	// Gap: 0001, 0003
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"chunked/0001": []byte("a"),
		"chunked/0003": []byte("b"),
	}, "seed invalid chunked tree")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	_, _, err = GetBytes(ctx, bs, "chunked", AllRange)
	require.Error(t, err)
	require.False(t, IsNotFoundError(err))
}

type interceptingAPI struct {
	git.GitAPI
	BlobReaderCalls int
	BlobSizesCalls  int
	BlobSizeCalls   int
}

func (a *interceptingAPI) BlobReader(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
	a.BlobReaderCalls++
	return a.GitAPI.BlobReader(ctx, oid)
}

func (a *interceptingAPI) BlobSizes(ctx context.Context, oids []git.OID) ([]int64, error) {
	a.BlobSizesCalls++
	return a.GitAPI.BlobSizes(ctx, oids)
}

func (a *interceptingAPI) BlobSize(ctx context.Context, oid git.OID) (int64, error) {
	a.BlobSizeCalls++
	return a.GitAPI.BlobSize(ctx, oid)
}

var _ git.GitAPI = (*interceptingAPI)(nil)

func TestGitBlobstore_Regression_ReadsReinflateQuadratic(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	_, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := localRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 1024,
	})
	require.NoError(t, err)

	chunk1 := bytes.Repeat([]byte("a"), 1024)
	chunk2 := bytes.Repeat([]byte("b"), 1024)

	_, err = PutBytes(ctx, bs, "c1", chunk1)
	require.NoError(t, err)
	_, err = PutBytes(ctx, bs, "c2", chunk2)
	require.NoError(t, err)
	_, err = bs.Concatenate(ctx, "largefile", []string{"c1", "c2"})
	require.NoError(t, err)

	_, err = bs.CheckAndPutManifest(ctx, "", nil)
	require.NoError(t, err)

	bsRead, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity: testIdentity(),
	})
	require.NoError(t, err)

	api := &interceptingAPI{GitAPI: bsRead.api}
	bsRead.api = api

	for i := 0; i < 5; i++ {
		_, _, err := GetBytes(ctx, bsRead, "largefile", NewBlobRange(int64(i*10), 10))
		require.NoError(t, err)
	}

	if api.BlobSizesCalls > 1 {
		t.Fatalf("BlobSizes was called %d times, expected at most 1 (caching sizes)", api.BlobSizesCalls)
	}

	if api.BlobReaderCalls >= 5 {
		t.Fatalf("BlobReader was called %d times, expected OID-level caching", api.BlobReaderCalls)
	}
}
