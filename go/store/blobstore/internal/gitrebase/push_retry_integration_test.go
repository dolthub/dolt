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

package gitrebase_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitrebase "github.com/dolthub/dolt/go/store/blobstore/internal/gitrebase"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

const (
	doltDataRef           = "refs/dolt/data"
	doltRemoteTrackingRef = "refs/dolt/remotes/origin/data"
)

func requireGitOnPath(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
}

func testIdentity() *git.Identity {
	return &git.Identity{Name: "gitrebase integration test", Email: "gitrebase-integration@test.invalid"}
}

func commitAddFile(t *testing.T, ctx context.Context, api git.GitAPI, parent git.OID, hasParent bool, path string, data []byte, msg string) git.OID {
	t.Helper()

	// Use git's temp index approach (no working tree).
	f, err := osCreateTempIndex()
	require.NoError(t, err)
	defer f.cleanup()

	if hasParent {
		require.NoError(t, api.ReadTree(ctx, parent, f.path))
	} else {
		require.NoError(t, api.ReadTreeEmpty(ctx, f.path))
	}
	oid, err := api.HashObject(ctx, bytes.NewReader(data))
	require.NoError(t, err)
	require.NoError(t, api.UpdateIndexCacheInfo(ctx, f.path, "100644", oid, path))
	tree, err := api.WriteTree(ctx, f.path)
	require.NoError(t, err)
	var pptr *git.OID
	if hasParent {
		p := parent
		pptr = &p
	}
	commit, err := api.CommitTree(ctx, tree, pptr, msg, testIdentity())
	require.NoError(t, err)
	return commit
}

type tempIndex struct {
	path    string
	cleanup func()
}

func osCreateTempIndex() (*tempIndex, error) {
	f, err := os.CreateTemp("", "dolt-gitrebase-integration-index-")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	_ = f.Close()
	return &tempIndex{
		path: path,
		cleanup: func() {
			_ = os.Remove(path)
			_ = os.Remove(path + ".lock")
		},
	}, nil
}

func readRefPath(t *testing.T, ctx context.Context, api git.GitAPI, ref string, path string) []byte {
	t.Helper()
	commit, err := api.ResolveRefCommit(ctx, ref)
	require.NoError(t, err)
	blobOID, err := api.ResolvePathBlob(ctx, commit, path)
	require.NoError(t, err)
	rc, err := api.BlobReader(ctx, blobOID)
	require.NoError(t, err)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

func TestPushRetryAfterRemoteAdvances_FetchMergeThenPushSucceeds(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	origin, err := gitrepo.InitBareTemp(ctx, "")
	require.NoError(t, err)
	local, err := gitrepo.InitBareTemp(ctx, "")
	require.NoError(t, err)

	originRunner, err := git.NewRunner(origin.GitDir)
	require.NoError(t, err)
	originAPI := git.NewGitAPIImpl(originRunner)

	localRunner, err := git.NewRunner(local.GitDir)
	require.NoError(t, err)
	localAPI := git.NewGitAPIImpl(localRunner)

	// Seed origin with a base commit at refs/dolt/data.
	base := commitAddFile(t, ctx, originAPI, "", false, "base", []byte("base\n"), "base")
	require.NoError(t, originAPI.UpdateRef(ctx, doltDataRef, base, "seed"))

	// Configure local's remote.
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", origin.GitDir)
	require.NoError(t, err)

	// Initial fetch and fast-forward merge into local ref to establish a common base.
	require.NoError(t, localAPI.FetchRef(ctx, "origin", doltDataRef, doltRemoteTrackingRef))
	_, res, err := gitrebase.MergeRemoteTrackingIntoLocalRef(ctx, localAPI, doltDataRef, doltRemoteTrackingRef, testIdentity())
	require.NoError(t, err)
	require.Equal(t, gitrebase.MergeFastForward, res)

	// Local writes a new key (commit A).
	localParent, err := localAPI.ResolveRefCommit(ctx, doltDataRef)
	require.NoError(t, err)
	localCommit := commitAddFile(t, ctx, localAPI, localParent, true, "local", []byte("L\n"), "local write")
	require.NoError(t, localAPI.UpdateRef(ctx, doltDataRef, localCommit, "local write"))

	// Remote advances independently (commit B) based on base.
	parent := base
	remoteAdv := commitAddFile(t, ctx, originAPI, parent, true, "remote", []byte("R\n"), "remote advance")
	require.NoError(t, originAPI.UpdateRef(ctx, doltDataRef, remoteAdv, "advance"))

	// First push should fail (non-fast-forward).
	_, err = localRunner.Run(ctx, git.RunOptions{}, "push", "origin", doltDataRef+":"+doltDataRef)
	require.Error(t, err)
	var ce *git.CmdError
	require.ErrorAs(t, err, &ce)
	msg := strings.ToLower(string(ce.Output))
	require.True(t, strings.Contains(msg, "rejected") || strings.Contains(msg, "non-fast-forward") || strings.Contains(msg, "fetch first"), "unexpected push failure output: %s", msg)

	// Rebase: fetch remote head into remote-tracking ref, merge it into local, then retry push.
	require.NoError(t, localAPI.FetchRef(ctx, "origin", doltDataRef, doltRemoteTrackingRef))
	_, res2, err := gitrebase.MergeRemoteTrackingIntoLocalRef(ctx, localAPI, doltDataRef, doltRemoteTrackingRef, testIdentity())
	require.NoError(t, err)
	require.Equal(t, gitrebase.MergeMerged, res2)

	_, err = localRunner.Run(ctx, git.RunOptions{}, "push", "origin", doltDataRef+":"+doltDataRef)
	require.NoError(t, err)

	// Verify origin now contains both keys.
	require.Equal(t, []byte("L\n"), readRefPath(t, ctx, originAPI, doltDataRef, "local"))
	require.Equal(t, []byte("R\n"), readRefPath(t, ctx, originAPI, doltDataRef, "remote"))
	require.Equal(t, []byte("base\n"), readRefPath(t, ctx, originAPI, doltDataRef, "base"))
}
