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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type forbidPathResolutionGitAPI struct {
	git.GitAPI
	resolvePathObjectCalls atomic.Int64
	listTreeCalls          atomic.Int64
}

func (f *forbidPathResolutionGitAPI) ResolvePathObject(ctx context.Context, commit git.OID, path string) (git.OID, git.ObjectType, error) {
	f.resolvePathObjectCalls.Add(1)
	return "", git.ObjectTypeUnknown, errors.New("forbidden call: ResolvePathObject")
}

func (f *forbidPathResolutionGitAPI) ListTree(ctx context.Context, commit git.OID, treePath string) ([]git.TreeEntry, error) {
	f.listTreeCalls.Add(1)
	return nil, errors.New("forbidden call: ListTree")
}

func TestGitBlobstore_ReadsUseCacheOnly_NoPathResolutionPlumbing(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	// Seed the remote with:
	// - an inline blob
	// - a chunked-tree representation (chunk/0001, chunk/0002)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest":   []byte("hello\n"),
		"dir/file":   []byte("abc"),
		"chunk/0001": []byte("abc"),
		"chunk/0002": []byte("def"),
		"chunk/0003": []byte("ghi"),
	}, "seed remote")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)

	wrapped := &forbidPathResolutionGitAPI{GitAPI: bs.api}
	bs.api = wrapped

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)

	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Chunked tree read: should be served via cache for path resolution (and stream part blobs by OID).
	gotChunk, _, err := GetBytes(ctx, bs, "chunk", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("abcdefghi"), gotChunk)

	require.Equal(t, int64(0), wrapped.resolvePathObjectCalls.Load(), "expected no ResolvePathObject calls during reads")
	require.Equal(t, int64(0), wrapped.listTreeCalls.Load(), "expected no ListTree calls during reads")
}
