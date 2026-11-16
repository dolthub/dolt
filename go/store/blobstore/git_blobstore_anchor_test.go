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
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

func TestGitBlobstore_Teardown_PreservesIncrementalFetch(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, runner := newRemoteAndLocalRepos(t, ctx)
	data := make([]byte, 1024*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)
	first, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("first"), "table": data}, "first")
	require.NoError(t, err)
	bs, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.NoError(t, bs.Teardown(ctx))
	api := git.NewGitAPIImpl(runner)
	anchor := RemoteTrackingRef("origin", DoltDataRef, "last")
	oid, err := api.ResolveRefCommit(ctx, anchor)
	require.NoError(t, err)
	require.Equal(t, git.OID(first), oid)
	_, exists, err := api.TryResolveRefCommit(ctx, bs.remoteTrackingRef)
	require.NoError(t, err)
	require.False(t, exists)

	// Make a small parented update with the original large table unchanged.
	second, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("second"), "table": data}, "second")
	require.NoError(t, err)
	rr, err := git.NewRunner(remote.GitDir)
	require.NoError(t, err)
	tree, err := rr.Run(ctx, git.RunOptions{}, "rev-parse", second+"^{tree}")
	require.NoError(t, err)
	parent := git.OID(first)
	remoteAPI := git.NewGitAPIImpl(rr)
	head, err := remoteAPI.CommitTree(ctx, git.OID(string(bytes.TrimSpace(tree))), &parent, "incremental", testIdentity())
	require.NoError(t, err)
	require.NoError(t, remoteAPI.UpdateRef(ctx, DoltDataRef, head, "advance"))

	// A new instance must negotiate using the anchor left by teardown.
	reopened, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	pack := filepath.Join(t.TempDir(), "received.pack")
	reopened.api = git.NewGitAPIImpl(runner.WithExtraEnv("GIT_TRACE_PACKFILE=" + pack))
	_, err = reopened.Exists(ctx, "manifest")
	require.NoError(t, err)
	stat, err := os.Stat(pack)
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(0))
	require.Less(t, stat.Size(), int64(16*1024), "unchanged 1 MiB table should not be retransmitted")
	require.NoError(t, reopened.Teardown(ctx))
}

func TestGitBlobstore_Teardown_OlderSessionCannotReplaceAnchor(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, runner := newRemoteAndLocalRepos(t, ctx)
	oldHead, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("old")}, "old")
	require.NoError(t, err)
	seed, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = seed.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.NoError(t, seed.Teardown(ctx))
	old, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = old.Exists(ctx, "manifest")
	require.NoError(t, err)

	// Both sessions observed the same anchor before either updated it. The newer tip
	// is deliberately parentless, so retaining the old tip would pin old history.
	newHead, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("new")}, "new")
	require.NoError(t, err)
	newer, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = newer.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.NoError(t, newer.Teardown(ctx))
	require.NoError(t, old.Teardown(ctx))
	api := git.NewGitAPIImpl(runner)
	oid, err := api.ResolveRefCommit(ctx, RemoteTrackingRef("origin", DoltDataRef, "last"))
	require.NoError(t, err)
	require.Equal(t, git.OID(newHead), oid)
	_, exists, err := api.TryResolveRefCommit(ctx, old.remoteTrackingRef)
	require.NoError(t, err)
	require.False(t, exists)
	reachable, err := runner.Run(ctx, git.RunOptions{}, "rev-list", "--all")
	require.NoError(t, err)
	require.NotContains(t, string(reachable), oldHead, "replacing the anchor must release disconnected history")
}

func TestGitBlobstore_Teardown_AnchorFailureCleansPrivateRefs(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, runner := newRemoteAndLocalRepos(t, ctx)
	_, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed")}, "seed")
	require.NoError(t, err)
	bs, err := NewGitBlobstore(local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	head, err := api.ResolveRefCommit(ctx, bs.remoteTrackingRef)
	require.NoError(t, err)
	require.NoError(t, api.UpdateRef(ctx, bs.localRef, head, "seed private local ref"))
	anchor := RemoteTrackingRef("origin", DoltDataRef, "last")
	lock := filepath.Join(local.GitDir, filepath.FromSlash(anchor)+".lock")
	require.NoError(t, os.WriteFile(lock, nil, 0600))
	require.Error(t, bs.Teardown(ctx))
	for _, ref := range []string{bs.localRef, bs.remoteTrackingRef} {
		_, exists, err := api.TryResolveRefCommit(ctx, ref)
		require.NoError(t, err)
		require.False(t, exists, "teardown should remove private ref %s even when publishing the anchor fails", ref)
	}
	require.NoError(t, os.Remove(lock))
	require.NoError(t, bs.Teardown(ctx))
}

func TestGitBlobstore_Teardown_AnchorsSuccessfulPush(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	_, local, runner := newRemoteAndLocalRepos(t, ctx)
	bs, err := NewGitBlobstoreWithIdentity(local.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)
	_, err = bs.CheckAndPutManifest(ctx, "", []byte("seed"))
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	pushed, err := api.ResolveRefCommit(ctx, bs.localRef)
	require.NoError(t, err)
	require.NoError(t, bs.Teardown(ctx))
	anchored, err := api.ResolveRefCommit(ctx, RemoteTrackingRef("origin", DoltDataRef, "last"))
	require.NoError(t, err)
	require.Equal(t, pushed, anchored)
	_, exists, err := api.TryResolveRefCommit(ctx, bs.localRef)
	require.NoError(t, err)
	require.False(t, exists)
}
