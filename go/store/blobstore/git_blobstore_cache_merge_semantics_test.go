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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGitBlobstore_CacheMerge_ImmutableKeyStableOnceCached(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	// Seed remote with a key. This is representative of a tablefile key, which is
	// expected to be immutable.
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"k": []byte("A\n"),
	}, "seed A")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)

	gotA, verA, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("A\n"), gotA)
	require.NotEmpty(t, verA)

	// Simulate an external rewrite that repoints the same key to different bytes.
	// With merge-only cache semantics, once "k" is cached it should not be overwritten.
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"k": []byte("B\n"),
	}, "rewrite B")
	require.NoError(t, err)

	gotAfter, verAfter, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("A\n"), gotAfter)
	require.Equal(t, verA, verAfter)
}

func TestGitBlobstore_CacheMerge_ManifestUpdatesAcrossFetches(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("M1\n"),
	}, "seed M1")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)

	got1, ver1, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("M1\n"), got1)
	require.NotEmpty(t, ver1)

	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("M2\n"),
	}, "advance M2")
	require.NoError(t, err)

	got2, ver2, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("M2\n"), got2)
	require.NotEmpty(t, ver2)
	require.NotEqual(t, ver1, ver2)
}
