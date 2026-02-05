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
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitbs "github.com/dolthub/dolt/go/store/blobstore/internal/gitbs"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_Get_ChunkedDescriptor_AllAndRanges(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	runner, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	// Create two part blobs.
	part1 := []byte("abc")
	part2 := []byte("defgh")
	oid1, err := api.HashObject(ctx, bytes.NewReader(part1))
	require.NoError(t, err)
	oid2, err := api.HashObject(ctx, bytes.NewReader(part2))
	require.NoError(t, err)

	desc := gitbs.Descriptor{
		TotalSize: uint64(len(part1) + len(part2)),
		Parts: []gitbs.PartRef{
			{OIDHex: oid1.String(), Size: uint64(len(part1))},
			{OIDHex: oid2.String(), Size: uint64(len(part2))},
		},
	}
	descBytes, err := gitbs.EncodeDescriptor(desc)
	require.NoError(t, err)
	descOID, err := api.HashObject(ctx, bytes.NewReader(descBytes))
	require.NoError(t, err)

	// Build a commit whose tree contains:
	// - key "chunked" -> descriptor blob
	// - parts staged under reserved parts namespace (reachability)
	_, indexFile, cleanup, err := newTempIndex()
	require.NoError(t, err)
	defer cleanup()

	require.NoError(t, api.ReadTreeEmpty(ctx, indexFile))
	require.NoError(t, api.UpdateIndexCacheInfo(ctx, indexFile, "100644", descOID, "chunked"))
	_, err = stagePartReachable(ctx, api, indexFile, oid1)
	require.NoError(t, err)
	_, err = stagePartReachable(ctx, api, indexFile, oid2)
	require.NoError(t, err)

	treeOID, err := api.WriteTree(ctx, indexFile)
	require.NoError(t, err)
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "seed chunked descriptor", &git.Identity{Name: "t", Email: "t@t"})
	require.NoError(t, err)
	require.NoError(t, api.UpdateRef(ctx, DoltDataRef, commitOID, "seed"))

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
	require.NoError(t, err)

	wantAll := append(append([]byte(nil), part1...), part2...)

	got, ver, err := GetBytes(ctx, bs, "chunked", AllRange)
	require.NoError(t, err)
	require.Equal(t, commitOID.String(), ver)
	require.Equal(t, wantAll, got)

	// Range spanning boundary: offset 2 length 4 => "cdef"
	got, ver, err = GetBytes(ctx, bs, "chunked", NewBlobRange(2, 4))
	require.NoError(t, err)
	require.Equal(t, commitOID.String(), ver)
	require.Equal(t, []byte("cdef"), got)

	// Tail read last 3 bytes => "fgh"
	got, ver, err = GetBytes(ctx, bs, "chunked", NewBlobRange(-3, 0))
	require.NoError(t, err)
	require.Equal(t, commitOID.String(), ver)
	require.Equal(t, []byte("fgh"), got)

	// Validate size returned is logical size, not descriptor size.
	rc, sz, ver2, err := bs.Get(ctx, "chunked", NewBlobRange(0, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(len(wantAll)), sz)
	require.Equal(t, commitOID.String(), ver2)
	_ = rc.Close()

	// Also verify "inline blob that happens to start with magic" is treated as inline
	// if it doesn't match the descriptor prefix (magic + size line).
	inline := "DOLTBS1\nthis is not a descriptor\n"
	inlineCommit, err := repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"inline": []byte(inline),
	}, "seed inline magic")
	require.NoError(t, err)

	bs2, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
	require.NoError(t, err)
	got2, ver3, err := GetBytes(ctx, bs2, "inline", AllRange)
	require.NoError(t, err)
	require.Equal(t, inlineCommit, ver3)
	require.Equal(t, []byte(inline), got2)
}
