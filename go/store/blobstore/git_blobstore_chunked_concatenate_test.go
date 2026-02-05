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
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitbs "github.com/dolthub/dolt/go/store/blobstore/internal/gitbs"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_Concatenate_ChunkedStructuralAndRechunksOversizedInline(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	// Seed an oversized inline blob (chunking disabled) so we exercise re-chunking during Concatenate.
	seed, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 0,
	})
	require.NoError(t, err)

	inline := []byte("abcdefghij") // 10 bytes
	_, err = seed.Put(ctx, "a", int64(len(inline)), bytes.NewReader(inline))
	require.NoError(t, err)

	// Now concatenate in chunked mode with a small max part size.
	bs, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	ver, err := bs.Concatenate(ctx, "out", []string{"a"})
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	got, ver2, err := GetBytes(ctx, bs, "out", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, inline, got)

	// Verify "out" is a descriptor and all parts are <= 3 and reachable under parts namespace.
	runner, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	commit := git.OID(ver)

	outOID, err := api.ResolvePathBlob(ctx, commit, "out")
	require.NoError(t, err)
	rc, err := api.BlobReader(ctx, outOID)
	require.NoError(t, err)
	descBytes, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	desc, err := gitbs.ParseDescriptor(descBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(len(inline)), desc.TotalSize)
	for _, p := range desc.Parts {
		require.LessOrEqual(t, p.Size, uint64(3))
		ppath, err := gitbs.PartPath(p.OIDHex)
		require.NoError(t, err)
		_, err = api.ResolvePathBlob(ctx, commit, ppath)
		require.NoError(t, err)
	}
}
