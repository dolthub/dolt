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
	"context"
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
