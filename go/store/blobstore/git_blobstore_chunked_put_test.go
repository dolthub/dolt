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
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	want := []byte("abcdefghij") // 10 bytes -> 3,3,3,1
	ver, err := bs.Put(ctx, "big", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	// Non-manifest Put is deferred; data should be readable from cache/local git objects.
	got, _, err := GetBytes(ctx, bs, "big", AllRange)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Flush deferred writes via CheckAndPut("manifest").
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	runner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	head, ok, err := api.TryResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.True(t, ok)

	_, typ, err := api.ResolvePathObject(ctx, head, "big")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeTree, typ)

	entries, err := api.ListTree(ctx, head, "big")
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.Equal(t, "0001", entries[0].Name)
	require.Equal(t, "0004", entries[3].Name)
}

func TestGitBlobstore_Put_IdempotentDoesNotChangeExistingRepresentation(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	runner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	// blob stays blob (even if the caller would have triggered chunked mode).
	// Non-manifest Put is deferred; idempotent re-Put should return same version from cache.
	verBlob, err := bs.Put(ctx, "k", 2, bytes.NewReader([]byte("hi")))
	require.NoError(t, err)
	verNoop, err := bs.Put(ctx, "k", 10, putShouldNotRead{})
	require.NoError(t, err)
	require.Equal(t, verBlob, verNoop)

	got, _, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), got)

	// tree stays tree
	verTree, err := bs.Put(ctx, "ktree", 10, bytes.NewReader([]byte("abcdefghij")))
	require.NoError(t, err)
	verTreeNoop, err := bs.Put(ctx, "ktree", 2, putShouldNotRead{})
	require.NoError(t, err)
	require.Equal(t, verTree, verTreeNoop)

	got, _, err = GetBytes(ctx, bs, "ktree", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("abcdefghij"), got)

	// Flush deferred writes and verify remote state.
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	head, ok, err := api.TryResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.True(t, ok)

	_, typ, err := api.ResolvePathObject(ctx, head, "k")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeBlob, typ)

	_, typ, err = api.ResolvePathObject(ctx, head, "ktree")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeTree, typ)
}
