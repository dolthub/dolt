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

func TestGitBlobstore_Put_ChunkedUnderMaxPartSize(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	want := []byte("abcdefghij") // 10 bytes -> 3,3,3,1
	ver, err := bs.Put(ctx, "big", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)

	got, ver2, err := GetBytes(ctx, bs, "big", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, want, got)

	runner, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	commit := git.OID(ver)
	keyOID, err := api.ResolvePathBlob(ctx, commit, "big")
	require.NoError(t, err)

	rc, err := api.BlobReader(ctx, keyOID)
	require.NoError(t, err)
	descBytes, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	desc, err := gitbs.ParseDescriptor(descBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(len(want)), desc.TotalSize)
	require.GreaterOrEqual(t, len(desc.Parts), 2)

	for _, p := range desc.Parts {
		require.LessOrEqual(t, p.Size, uint64(3))
		ppath, err := gitbs.PartPath(p.OIDHex)
		require.NoError(t, err)
		gotOID, err := api.ResolvePathBlob(ctx, commit, ppath)
		require.NoError(t, err)
		require.Equal(t, git.OID(p.OIDHex), gotOID)
	}

	// Range spanning boundary (offset 2, length 4) => "cdef"
	got, _, err = GetBytes(ctx, bs, "big", NewBlobRange(2, 4))
	require.NoError(t, err)
	require.Equal(t, []byte("cdef"), got)
}
