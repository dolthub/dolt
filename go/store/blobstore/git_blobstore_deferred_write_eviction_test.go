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

// A deferred (not-yet-manifested) table file must survive the post-flush cache
// eviction in remoteManagedWrite.
func TestGitBlobstore_PostFlushEviction_RacesDeferredWrite(t *testing.T) {
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

	// MaxPartSize forces the chunked-tree representation for table files, matching
	// the ".darc" archives in the report (stored as <key>/0001, <key>/0002, ...).
	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName:  "origin",
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	// Upload two chunked table files as deferred writes (like WriteTableFile).
	_, err = bs.Put(ctx, "A.darc", 10, bytes.NewReader([]byte("aaaaaaaaaa")))
	require.NoError(t, err)
	_, err = bs.Put(ctx, "B.darc", 10, bytes.NewReader([]byte("bbbbbbbbbb")))
	require.NoError(t, err)

	// Sanity: B is readable from cache before any manifest flush.
	got, _, err := GetBytes(ctx, bs, "B.darc", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("bbbbbbbbbb"), got)

	// First manifest flush references only A (the "window": B uploaded, not yet
	// manifested). Both deferred writes land in the tree, so B is now unreferenced.
	m1 := []byte("5:__DOLT__:lock1:root1:gc1:A:10")
	_, err = bs.CheckAndPut(ctx, "", "manifest", int64(len(m1)), bytes.NewReader(m1))
	require.NoError(t, err)

	_, ver1, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Second manifest flush still references only A. Now B exists in the parent
	// tree and is unreferenced -> pruned -> orphan commit -> post-flush eviction.
	m2 := []byte("5:__DOLT__:lock2:root2:gc2:A:10")
	_, err = bs.CheckAndPut(ctx, ver1, "manifest", int64(len(m2)), bytes.NewReader(m2))
	require.NoError(t, err)

	// B is unreferenced but never committed, so it stays pending and cached.
	// Before the fix it was committed then pruned+evicted, failing with
	// "Blob not found: B.darc".
	got, _, err = GetBytes(ctx, bs, "B.darc", AllRange)
	require.NoError(t, err, "live table file evicted mid-flight -> Blob not found")
	require.Equal(t, []byte("bbbbbbbbbb"), got)
}
