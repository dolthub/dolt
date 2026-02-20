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
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func requireGitOnPath(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
}

func testIdentity() *git.Identity {
	return &git.Identity{Name: "gitblobstore test", Email: "gitblobstore@test.invalid"}
}

func newRemoteAndLocalRepos(t *testing.T, ctx context.Context) (remoteRepo *gitrepo.Repo, localRepo *gitrepo.Repo, localRunner *git.Runner) {
	t.Helper()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	localRepo, err = gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err = git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)
	return remoteRepo, localRepo, localRunner
}

func TestGitBlobstore_MissingKeysAreNotFound(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.False(t, ok)

	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))

	_, _, _, err = bs.Get(ctx, "table", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_ExistsAndGet_AllRange(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	want := []byte("hello manifest\n")
	commit, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": want,
		"dir/file": []byte("abc"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)
	manifestOID, _, err := remoteAPI.ResolvePathObject(ctx, git.OID(commit), "manifest")
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Exists(ctx, "missing")
	require.NoError(t, err)
	require.False(t, ok)

	// Validate key normalization: backslash -> slash.
	ok, err = bs.Exists(ctx, "dir\\file")
	require.NoError(t, err)
	require.True(t, ok)

	got, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, manifestOID.String(), ver)
	require.Equal(t, want, got)

	// Validate size + version on Get.
	rc, sz, ver2, err := bs.Get(ctx, "manifest", NewBlobRange(0, 5))
	require.NoError(t, err)
	require.Equal(t, uint64(len(want)), sz)
	require.Equal(t, manifestOID.String(), ver2)
	_ = rc.Close()
}

func TestGitBlobstore_RemoteManaged_ExistsFetchesAndAligns(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteCommit, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("from remote\n"),
	}, "seed remote")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)

	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, DoltDataRef, bs.remoteRef)
	require.True(t, strings.HasPrefix(bs.remoteTrackingRef, "refs/dolt/remotes/origin/dolt/data/"))
	require.NotEqual(t, bs.remoteRef, bs.localRef)
	require.NotEqual(t, bs.remoteTrackingRef, bs.localRef)
	require.True(t, strings.HasPrefix(bs.localRef, "refs/dolt/blobstore/origin/dolt/data/"))

	localAPI := git.NewGitAPIImpl(localRunner)
	gotLocal, err := localAPI.ResolveRefCommit(ctx, bs.localRef)
	require.NoError(t, err)
	require.Equal(t, git.OID(remoteCommit), gotLocal)

	gotTracking, err := localAPI.ResolveRefCommit(ctx, bs.remoteTrackingRef)
	require.NoError(t, err)
	require.Equal(t, git.OID(remoteCommit), gotTracking)
}

func TestGitBlobstore_RemoteAndLocalRefNaming_ConfigurableRemoteRef(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)

	const remoteRef = "refs/heads/alt"
	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, remoteRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)

	require.Equal(t, remoteRef, bs.remoteRef)
	require.True(t, strings.HasPrefix(bs.remoteTrackingRef, "refs/dolt/remotes/origin/heads/alt/"))
	require.NotEmpty(t, bs.localRef)
	require.NotEqual(t, bs.remoteRef, bs.localRef)
	require.NotEqual(t, bs.remoteTrackingRef, bs.localRef)
	require.True(t, strings.HasPrefix(bs.localRef, "refs/dolt/blobstore/origin/heads/alt/"))
}

func TestGitBlobstore_TwoInstances_IndependentTrackingRefs(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("v1\n"),
	}, "seed")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	opts := GitBlobstoreOptions{RemoteName: "origin"}
	bs1, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, opts)
	require.NoError(t, err)
	bs2, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, opts)
	require.NoError(t, err)

	// The two instances must have distinct tracking and local refs.
	require.NotEqual(t, bs1.remoteTrackingRef, bs2.remoteTrackingRef)
	require.NotEqual(t, bs1.localRef, bs2.localRef)

	// Both instances can fetch independently without interfering.
	ok1, err := bs1.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok1)

	ok2, err := bs2.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok2)

	// Verify each instance wrote to its own tracking ref.
	localAPI := git.NewGitAPIImpl(localRunner)
	head1, err := localAPI.ResolveRefCommit(ctx, bs1.remoteTrackingRef)
	require.NoError(t, err)
	head2, err := localAPI.ResolveRefCommit(ctx, bs2.remoteTrackingRef)
	require.NoError(t, err)
	require.Equal(t, head1, head2, "both should track the same remote commit")

	// Verify local refs are also distinct and valid.
	local1, err := localAPI.ResolveRefCommit(ctx, bs1.localRef)
	require.NoError(t, err)
	local2, err := localAPI.ResolveRefCommit(ctx, bs2.localRef)
	require.NoError(t, err)
	require.Equal(t, local1, local2, "both should point at the same commit")
}

func TestGitBlobstore_CleanupOwnedLocalRef_DeletesRef(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	_, err = localRepo.SetRefToTree(ctx, bs.localRef, map[string][]byte{
		"manifest": []byte("x"),
	}, "seed localRef")
	require.NoError(t, err)

	runner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	_, err = api.ResolveRefCommit(ctx, bs.localRef)
	require.NoError(t, err)

	require.NoError(t, bs.CleanupOwnedLocalRef(ctx))

	_, err = api.ResolveRefCommit(ctx, bs.localRef)
	var rnf *git.RefNotFoundError
	require.ErrorAs(t, err, &rnf)
	require.Equal(t, bs.localRef, rnf.Ref)
}

func TestGitBlobstore_RemoteManaged_PutPushesToRemote(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"base": []byte("base\n"),
	}, "seed remote")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	ver, err := PutBytes(ctx, bs, "k", []byte("from local\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	// Non-manifest Put is deferred; flush via CheckAndPut("manifest").
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	oid, typ, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "k")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeBlob, typ)

	rc, err := remoteAPI.BlobReader(ctx, oid)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, []byte("from local\n"), got)
}

func TestGitBlobstore_RemoteManaged_PutBootstrapsEmptyRemote(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	// Do not seed refs/dolt/data in the remote: simulate a truly empty remote.
	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	want := []byte("bootstrapped\n")
	ver, err := bs.Put(ctx, "k", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	// Non-manifest Put is deferred; flush via CheckAndPut("manifest").
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	// Remote should now have refs/dolt/data and contain the key.
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.NotEmpty(t, remoteHead)

	oid, typ, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "k")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeBlob, typ)

	rc, err := remoteAPI.BlobReader(ctx, oid)
	require.NoError(t, err)
	got, rerr := io.ReadAll(rc)
	_ = rc.Close()
	require.NoError(t, rerr)
	require.Equal(t, want, got)
}

type hookPushGitAPI struct {
	git.GitAPI
	onFirstPush func(ctx context.Context)
	did         atomic.Bool
}

func (h *hookPushGitAPI) PushRefWithLease(ctx context.Context, remote string, srcRef string, dstRef string, expectedDstOID git.OID) error {
	if h.onFirstPush != nil && !h.did.Swap(true) {
		h.onFirstPush(ctx)
	}
	return h.GitAPI.PushRefWithLease(ctx, remote, srcRef, dstRef, expectedDstOID)
}

func TestGitBlobstore_RemoteManaged_PutRetriesOnLeaseFailure(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Seed remote so it has a head for the lease.
	_, err = writeKeyToRef(ctx, remoteAPI, DoltDataRef, "base", []byte("base\n"), testIdentity())
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	var externalHead atomic.Value // git.OID

	// Advance the remote right before the first push to force a lease failure and trigger a retry.
	bs.api = &hookPushGitAPI{
		GitAPI: bs.api,
		onFirstPush: func(ctx context.Context) {
			oid, _ := writeKeyToRef(ctx, remoteAPI, DoltDataRef, "external", []byte("external\n"), testIdentity())
			if oid != "" {
				externalHead.Store(oid)
			}
		},
	}

	// Put is deferred (no push). The retry happens during CheckAndPut flush.
	ver, err := PutBytes(ctx, bs, "k", []byte("after retry\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)

	// Verify we rebuilt on top of the advanced remote head (i.e. parent is externalHead).
	if v := externalHead.Load(); v != nil {
		wantParent := v.(git.OID)
		out, err := remoteRunner.Run(ctx, git.RunOptions{}, "rev-parse", remoteHead.String()+"^")
		require.NoError(t, err)
		require.Equal(t, wantParent.String(), string(bytes.TrimSpace(out)))
		_, err = remoteRunner.Run(ctx, git.RunOptions{}, "rev-parse", remoteHead.String()+"^2")
		require.Error(t, err) // not a merge commit
	}

	oid, typ, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "k")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeBlob, typ)

	rc, err := remoteAPI.BlobReader(ctx, oid)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, []byte("after retry\n"), got)
}

func TestGitBlobstore_RemoteManaged_CheckAndPut_RemoteHeadTruth(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Base manifest
	base, err := writeKeyToRef(ctx, remoteAPI, DoltDataRef, "manifest", []byte("base\n"), testIdentity())
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)
	localAPI := git.NewGitAPIImpl(localRunner)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	// Fetch remote so local has the base object, then create a conflicting local commit.
	require.NoError(t, localAPI.FetchRef(ctx, "origin", DoltDataRef, bs.remoteTrackingRef))
	require.NoError(t, localAPI.UpdateRef(ctx, bs.localRef, base, "set local to base"))
	_, err = writeKeyToRef(ctx, localAPI, bs.localRef, "manifest", []byte("local\n"), testIdentity())
	require.NoError(t, err)

	// Advance remote independently so we have a conflict on "manifest".
	_, err = writeKeyToRef(ctx, remoteAPI, DoltDataRef, "manifest", []byte("remote\n"), testIdentity())
	require.NoError(t, err)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	remoteManifestOID, _, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "manifest")
	require.NoError(t, err)

	// Remote is truth: CheckAndPut validates against remoteHead and applies changes on top of it.
	newBytes := []byte("replayed\n")
	ver, err := bs.CheckAndPut(ctx, remoteManifestOID.String(), "manifest", int64(len(newBytes)), bytes.NewReader(newBytes))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	remoteHead, err = remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	oid, _, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "manifest")
	require.NoError(t, err)
	rc, err := remoteAPI.BlobReader(ctx, oid)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, newBytes, got)
}

func TestGitBlobstore_RemoteManaged_CheckAndPut_ExpectedMatchesLocalButNotRemoteFails(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Base manifest
	base, err := writeKeyToRef(ctx, remoteAPI, DoltDataRef, "manifest", []byte("base\n"), testIdentity())
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)
	localAPI := git.NewGitAPIImpl(localRunner)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	// Create a local-only manifest version.
	require.NoError(t, localAPI.FetchRef(ctx, "origin", DoltDataRef, bs.remoteTrackingRef))
	require.NoError(t, localAPI.UpdateRef(ctx, bs.localRef, base, "set local to base"))
	_, err = writeKeyToRef(ctx, localAPI, bs.localRef, "manifest", []byte("local\n"), testIdentity())
	require.NoError(t, err)
	localHead, err := localAPI.ResolveRefCommit(ctx, bs.localRef)
	require.NoError(t, err)
	localManifestOID, _, err := localAPI.ResolvePathObject(ctx, localHead, "manifest")
	require.NoError(t, err)

	// Advance remote independently.
	_, err = writeKeyToRef(ctx, remoteAPI, DoltDataRef, "manifest", []byte("remote\n"), testIdentity())
	require.NoError(t, err)
	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	remoteManifestOID, _, err := remoteAPI.ResolvePathObject(ctx, remoteHead, "manifest")
	require.NoError(t, err)

	// Expected version matches local, but remote is truth, so this should fail.
	_, err = bs.CheckAndPut(ctx, localManifestOID.String(), "manifest", int64(len("new\n")), bytes.NewReader([]byte("new\n")))
	var capErr CheckAndPutError
	require.ErrorAs(t, err, &capErr)
	require.Equal(t, "manifest", capErr.Key)
	require.Equal(t, localManifestOID.String(), capErr.ExpectedVersion)
	require.Equal(t, remoteManifestOID.String(), capErr.ActualVersion)
}

func TestGitBlobstore_RemoteManaged_PutOverwritesDivergedLocalRef_NoMergeCommit(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Seed + advance remote.
	_, err = writeKeyToRef(ctx, remoteAPI, DoltDataRef, "base", []byte("base\n"), testIdentity())
	require.NoError(t, err)
	_, err = writeKeyToRef(ctx, remoteAPI, DoltDataRef, "remote", []byte("remote\n"), testIdentity())
	require.NoError(t, err)
	remoteHeadBefore, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)
	localAPI := git.NewGitAPIImpl(localRunner)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	// Make local diverge from remote.
	require.NoError(t, localAPI.FetchRef(ctx, "origin", DoltDataRef, bs.remoteTrackingRef))
	require.NoError(t, localAPI.UpdateRef(ctx, bs.localRef, remoteHeadBefore, "set local to remote head"))
	_, err = writeKeyToRef(ctx, localAPI, bs.localRef, "local", []byte("local\n"), testIdentity())
	require.NoError(t, err)

	_, err = PutBytes(ctx, bs, "k", []byte("from local\n"))
	require.NoError(t, err)

	// Non-manifest Put is deferred; flush via CheckAndPut("manifest").
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	remoteHeadAfter, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)

	// New remote head is a normal (non-merge) commit built on remoteHeadBefore.
	out, err := remoteRunner.Run(ctx, git.RunOptions{}, "rev-parse", remoteHeadAfter.String()+"^")
	require.NoError(t, err)
	require.Equal(t, remoteHeadBefore.String(), string(bytes.TrimSpace(out)))
	_, err = remoteRunner.Run(ctx, git.RunOptions{}, "rev-parse", remoteHeadAfter.String()+"^2")
	require.Error(t, err)

	// Local-only divergence should not be present on remote.
	_, _, err = remoteAPI.ResolvePathObject(ctx, remoteHeadAfter, "local")
	require.Error(t, err)
}

func TestGitBlobstore_Get_NotFoundMissingKey(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"present": []byte("x"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	_, _, err = GetBytes(ctx, bs, "missing", AllRange)
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
}

func TestGitBlobstore_BlobRangeSemantics(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	maxValue := int64(16 * 1024)
	testData := rangeData(0, maxValue)

	commit, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"range": testData,
	}, "range fixture")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(localRepo.GitDir, DoltDataRef)
	require.NoError(t, err)

	runner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	rangeOID, _, err := api.ResolvePathObject(ctx, git.OID(commit), "range")
	require.NoError(t, err)

	// full range
	got, ver, err := GetBytes(ctx, bs, "range", AllRange)
	require.NoError(t, err)
	require.Equal(t, rangeOID.String(), ver)
	require.Equal(t, rangeData(0, maxValue), got)

	// first 2048 bytes (1024 shorts)
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(0, 2048))
	require.NoError(t, err)
	require.Equal(t, rangeOID.String(), ver)
	require.Equal(t, rangeData(0, 1024), got)

	// bytes 2048..4096 of original
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(2*1024, 2*1024))
	require.NoError(t, err)
	require.Equal(t, rangeOID.String(), ver)
	require.Equal(t, rangeData(1024, 2048), got)

	// last 2048 bytes
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(-2*1024, 0))
	require.NoError(t, err)
	require.Equal(t, rangeOID.String(), ver)
	require.Equal(t, rangeData(maxValue-1024, maxValue), got)

	// tail slice: beginning 2048 bytes from end, size 512
	got, ver, err = GetBytes(ctx, bs, "range", NewBlobRange(-2*1024, 512))
	require.NoError(t, err)
	require.Equal(t, rangeOID.String(), ver)
	require.Equal(t, rangeData(maxValue-1024, maxValue-768), got)
}

func TestGitBlobstore_InvalidKeysError(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	_, err = repo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"ok": []byte("x")}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstore(repo.GitDir, DoltDataRef)
	require.NoError(t, err)

	invalid := []string{
		"",
		"/abs",
		"../x",
		"a/../b",
		"a//b",
		"a/",
		".",
		"..",
		"a/./b",
		"a/\x00/b",
	}

	for _, k := range invalid {
		_, err := bs.Exists(ctx, k)
		require.Error(t, err, "expected error for key %q", k)

		_, _, _, err = bs.Get(ctx, k, AllRange)
		require.Error(t, err, "expected error for key %q", k)

		_, err = bs.Put(ctx, k, 1, bytes.NewReader([]byte("x")))
		require.Error(t, err, "expected error for key %q", k)
	}
}

func TestGitBlobstore_Put_RoundTripAndVersion(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	want := []byte("hello put\n")
	ver, err := PutBytes(ctx, bs, "k", want)
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	ok, err := bs.Exists(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok)

	got, ver2, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, want, got)
}

func TestGitBlobstore_Concatenate_Basic(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	_, err = PutBytes(ctx, bs, "a", []byte("hi "))
	require.NoError(t, err)
	_, err = PutBytes(ctx, bs, "b", []byte("there"))
	require.NoError(t, err)

	ver, err := bs.Concatenate(ctx, "c", []string{"a", "b"})
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	got, ver2, err := GetBytes(ctx, bs, "c", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, []byte("hi there"), got)
}

func TestGitBlobstore_Concatenate_ChunkedResult(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 1024,
	})
	require.NoError(t, err)

	a := bytes.Repeat([]byte("a"), 700)
	b := bytes.Repeat([]byte("b"), 700)
	want := append(append([]byte(nil), a...), b...)

	_, err = PutBytes(ctx, bs, "a", a)
	require.NoError(t, err)
	_, err = PutBytes(ctx, bs, "b", b)
	require.NoError(t, err)

	ver, err := bs.Concatenate(ctx, "c", []string{"a", "b"})
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	// Non-manifest writes are deferred; flush via CheckAndPut("manifest").
	_, err = bs.CheckAndPut(ctx, "", "manifest", 3, bytes.NewReader([]byte("m1\n")))
	require.NoError(t, err)

	// Verify the resulting key is stored as a chunked tree on the remote.
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)
	head, ok, err := remoteAPI.TryResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.True(t, ok)
	_, typ, err := remoteAPI.ResolvePathObject(ctx, head, "c")
	require.NoError(t, err)
	require.Equal(t, git.ObjectTypeTree, typ)

	parts, err := remoteAPI.ListTree(ctx, head, "c")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(parts), 2)
	require.Equal(t, "0001", parts[0].Name)

	got, _, err := GetBytes(ctx, bs, "c", AllRange)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestGitBlobstore_Concatenate_KeyExistsFastSucceeds(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	ver1, err := PutBytes(ctx, bs, "c", []byte("original"))
	require.NoError(t, err)
	require.NotEmpty(t, ver1)

	_, err = PutBytes(ctx, bs, "a", []byte("new "))
	require.NoError(t, err)
	_, err = PutBytes(ctx, bs, "b", []byte("value"))
	require.NoError(t, err)

	ver2, err := bs.Concatenate(ctx, "c", []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, ver1, ver2, "expected concatenate to fast-succeed without overwriting existing key")

	got, ver3, err := GetBytes(ctx, bs, "c", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver1, ver3)
	require.Equal(t, []byte("original"), got)
}

func TestGitBlobstore_Concatenate_MissingSourceIsNotFound(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"present": []byte("x"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	_, err = PutBytes(ctx, bs, "present", []byte("x"))
	require.NoError(t, err)

	_, err = bs.Concatenate(ctx, "c", []string{"present", "missing"})
	require.Error(t, err)
	require.True(t, IsNotFoundError(err))
	var nf NotFound
	require.ErrorAs(t, err, &nf)
	require.Equal(t, "missing", nf.Key)
}

func TestGitBlobstore_Concatenate_EmptySourcesErrors(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	_, err = bs.Concatenate(ctx, "c", nil)
	require.Error(t, err)
}

type putShouldNotRead struct{}

func (putShouldNotRead) Read(_ []byte) (int, error) {
	return 0, errors.New("read should not be called")
}

func TestGitBlobstore_Put_IdempotentIfKeyExists(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	ver1, err := PutBytes(ctx, bs, "k", []byte("v1\n"))
	require.NoError(t, err)
	require.NotEmpty(t, ver1)

	ver2, err := bs.Put(ctx, "k", 3, putShouldNotRead{})
	require.NoError(t, err)
	require.Equal(t, ver1, ver2)

	got, ver3, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver1, ver3)
	require.Equal(t, []byte("v1\n"), got)
}

func writeKeyToRef(ctx context.Context, api git.GitAPI, ref string, key string, data []byte, author *git.Identity) (git.OID, error) {
	parent, ok, err := api.TryResolveRefCommit(ctx, ref)
	if err != nil {
		return "", err
	}

	indexDir, err := os.MkdirTemp("", "gitblobstore-test-index-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(indexDir) }()
	indexFile := filepath.Join(indexDir, "index")

	if ok {
		if err := api.ReadTree(ctx, parent, indexFile); err != nil {
			return "", err
		}
	} else {
		if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
			return "", err
		}
	}

	blobOID, err := api.HashObject(ctx, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", blobOID, key); err != nil {
		return "", err
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		return "", err
	}

	var parentPtr *git.OID
	if ok && parent != "" {
		p := parent
		parentPtr = &p
	}

	msg := "test external writer"
	commitOID, err := api.CommitTree(ctx, treeOID, parentPtr, msg, author)
	if err != nil {
		return "", err
	}

	if err := api.UpdateRef(ctx, ref, commitOID, msg); err != nil {
		return "", err
	}
	return commitOID, nil
}

type failReader struct {
	called atomic.Bool
}

func (r *failReader) Read(_ []byte) (int, error) {
	r.called.Store(true)
	return 0, io.EOF
}

func TestGitBlobstore_CheckAndPut_CreateOnly(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	want := []byte("created\n")
	ver, err := bs.CheckAndPut(ctx, "", "k", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	got, ver2, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, want, got)
}

func TestGitBlobstore_CheckAndPut_MismatchDoesNotRead(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	commit, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"k": []byte("base\n"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	runner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	keyOID, _, err := api.ResolvePathObject(ctx, git.OID(commit), "k")
	require.NoError(t, err)

	r := &failReader{}
	_, err = bs.CheckAndPut(ctx, keyOID.String()+"-wrong", "k", 1, r)
	require.Error(t, err)
	require.True(t, IsCheckAndPutError(err))
	require.False(t, r.called.Load(), "expected reader not to be consumed on version mismatch")
}

func TestGitBlobstore_CheckAndPut_UpdateSuccess(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	commit, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"k":    []byte("base\n"),
		"keep": []byte("keep\n"),
	}, "seed")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithIdentity(localRepo.GitDir, DoltDataRef, testIdentity())
	require.NoError(t, err)

	runner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	keyOID, _, err := api.ResolvePathObject(ctx, git.OID(commit), "k")
	require.NoError(t, err)

	want := []byte("updated\n")
	ver2, err := bs.CheckAndPut(ctx, keyOID.String(), "k", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)
	require.NotEmpty(t, ver2)
	require.NotEqual(t, keyOID.String(), ver2)

	got, ver3, err := GetBytes(ctx, bs, "k", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver2, ver3)
	require.Equal(t, want, got)

	got, _, err = GetBytes(ctx, bs, "keep", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("keep\n"), got)
}
