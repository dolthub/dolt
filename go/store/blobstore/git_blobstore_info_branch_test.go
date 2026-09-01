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

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

// The info branch is force-pushed on every data write. It must NOT land under
// refs/heads/, or the default fetch refspec (+refs/heads/*:refs/remotes/origin/*)
// sweeps this high-churn ref into every consumer's `git fetch`, where concurrent
// fetches race git's compare-and-swap and fail with "incorrect old value
// provided". It must land under refs/dolt/info/ instead (still visible in
// `git ls-remote`, but not fetched by default). See dolthub/dolt#11648.
func TestGitBlobstore_InfoBranch_PushedOutsideRefsHeads(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
		InfoBranch: DefaultInfoBranch,
	})
	require.NoError(t, err)

	// A data write + manifest flush triggers the best-effort info-branch push.
	_, err = PutBytes(ctx, bs, "k", []byte("v\n"))
	require.NoError(t, err)
	_, err = bs.CheckAndPutManifest(ctx, "", []byte("manifest\n"))
	require.NoError(t, err)

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)

	// The info ref must exist under refs/dolt/info/ ...
	infoRefName := "refs/dolt/info/" + DefaultInfoBranch
	out, err := remoteRunner.Run(ctx, git.RunOptions{}, "show-ref", "--verify", infoRefName)
	require.NoError(t, err, "info ref %s should exist on the remote; show-ref out=%q", infoRefName, string(out))
	require.Contains(t, string(out), infoRefName)

	// ... and must NOT exist under refs/heads/ (where default fetch would sweep it).
	headsRefName := "refs/heads/" + DefaultInfoBranch
	_, err = remoteRunner.Run(ctx, git.RunOptions{}, "show-ref", "--verify", "--quiet", headsRefName)
	require.Error(t, err, "info branch must not be published under %s", headsRefName)

	// No branch should have leaked under refs/heads/ at all from the info push.
	branches, err := remoteRunner.Run(ctx, git.RunOptions{}, "for-each-ref", "--format=%(refname)", "refs/heads/")
	require.NoError(t, err)
	require.NotContains(t, string(branches), DefaultInfoBranch,
		"no refs/heads/ ref should carry the info branch name")
}

// A default `git clone` + `git fetch` of the remote must succeed without pulling
// the info branch into refs/remotes/origin/ (it lives outside refs/heads/), so
// the concurrent-fetch race on that ref cannot occur.
func TestGitBlobstore_InfoBranch_NotFetchedByDefault(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
		InfoBranch: DefaultInfoBranch,
	})
	require.NoError(t, err)
	_, err = PutBytes(ctx, bs, "k", []byte("v\n"))
	require.NoError(t, err)
	_, err = bs.CheckAndPutManifest(ctx, "", []byte("manifest\n"))
	require.NoError(t, err)

	// Fresh bare clone of the remote, then a default fetch.
	consumerRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/consumer.git")
	require.NoError(t, err)
	consumerRunner, err := git.NewRunner(consumerRepo.GitDir)
	require.NoError(t, err)
	_, err = consumerRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)
	_, err = consumerRunner.Run(ctx, git.RunOptions{}, "fetch", "origin")
	require.NoError(t, err, "default fetch of a remote with an info branch should succeed")

	// The info branch must not have been swept into refs/remotes/origin/.
	tracking, err := consumerRunner.Run(ctx, git.RunOptions{}, "for-each-ref", "--format=%(refname)", "refs/remotes/")
	require.NoError(t, err)
	require.NotContains(t, string(tracking), DefaultInfoBranch,
		"info branch should not be fetched into refs/remotes/origin/ by default")
}
