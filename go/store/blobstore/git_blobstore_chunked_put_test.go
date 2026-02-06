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
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_Put_ChunkedWritesTreeParts(t *testing.T) {
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
	_, typ, err := api.ResolvePathObject(ctx, commit, "big")
	require.NoError(t, err)
	require.Equal(t, "tree", typ)

	entries, err := api.ListTree(ctx, commit, "big")
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.Equal(t, "00000001", entries[0].Name)
	require.Equal(t, "00000004", entries[3].Name)
}

func TestGitBlobstore_Put_TreeToBlobAndBlobToTreeTransitions(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	runner, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	// blob -> tree
	_, err = bs.Put(ctx, "k", 2, bytes.NewReader([]byte("hi")))
	require.NoError(t, err)
	verTree, err := bs.Put(ctx, "k", 10, bytes.NewReader([]byte("abcdefghij")))
	require.NoError(t, err)
	_, typ, err := api.ResolvePathObject(ctx, git.OID(verTree), "k")
	require.NoError(t, err)
	require.Equal(t, "tree", typ)

	// tree -> blob
	verBlob, err := bs.Put(ctx, "k", 2, bytes.NewReader([]byte("ok")))
	require.NoError(t, err)
	_, typ, err = api.ResolvePathObject(ctx, git.OID(verBlob), "k")
	require.NoError(t, err)
	require.Equal(t, "blob", typ)

	got, _, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), got)
}
