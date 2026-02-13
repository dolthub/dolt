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

package nbs

import (
	"bytes"
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

func TestNBS_GitBlobstore_EmptyRemote_OpenReturnsEmptyManifest(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	// Do not seed refs/dolt/data in the remote: simulate a truly empty remote.
	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", localRepo.GitDir, "remote", "add", "origin", remoteRepo.GitDir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git remote add failed: %s", string(out))

	store, err := NewGitStore(ctx, types.Format_DOLT.VersionString(), localRepo.GitDir, blobstore.DoltDataRef, blobstore.GitBlobstoreOptions{}, 0, NewUnlimitedMemQuotaProvider())
	require.NoError(t, err)
	defer store.Close()

	exists, _, _, err := store.manifestMgr.Fetch(ctx, store.stats)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestNBS_GitBlobstore_EmptyRemote_FirstManifestUpdateBootstrapsRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	// Do not seed refs/dolt/data in the remote: simulate a truly empty remote.
	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", localRepo.GitDir, "remote", "add", "origin", remoteRepo.GitDir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git remote add failed: %s", string(out))

	bs, err := blobstore.NewGitBlobstore(localRepo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	// Write a valid v5 manifest into the empty remote via the blobstore manifest updater.
	root := hash.Of([]byte("root"))
	gcGen := hash.Hash{}
	want := manifestContents{
		nbfVers:  types.Format_DOLT.VersionString(),
		root:     root,
		gcGen:    gcGen,
		specs:    nil,
		appendix: nil,
	}
	want.lock = generateLockHash(want.root, want.specs, want.appendix, nil)

	stats := NewStats()
	got, err := blobstoreManifest{bs: bs}.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, want, stats, nil)
	require.NoError(t, err)
	require.Equal(t, want.lock, got.lock)
	require.Equal(t, want.root, got.root)

	// Remote ref should now exist.
	cmd = exec.CommandContext(ctx, "git", "--git-dir", remoteRepo.GitDir, "rev-parse", "--verify", "--quiet", blobstore.DoltDataRef+"^{commit}")
	revParseOut, err := cmd.CombinedOutput()
	require.NoError(t, err, "git rev-parse failed: %s", string(revParseOut))

	// Re-open via NBS and ensure manifest is readable.
	store, err := NewGitStore(ctx, types.Format_DOLT.VersionString(), localRepo.GitDir, blobstore.DoltDataRef, blobstore.GitBlobstoreOptions{}, 0, NewUnlimitedMemQuotaProvider())
	require.NoError(t, err)
	defer store.Close()

	exists, contents, _, err := store.manifestMgr.Fetch(ctx, store.stats)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, want.root, contents.root)
	require.Equal(t, want.lock, contents.lock)

	// Sanity: manifest blob contents are parseable.
	var buf bytes.Buffer
	require.NoError(t, writeManifest(&buf, contents))
	require.NotEmpty(t, buf.Bytes())
}

func TestNBS_NewGitStore_DefaultsMaxPartSizeTo50MB(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, blobstore.DoltDataRef, nil, "seed empty")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", localRepo.GitDir, "remote", "add", "origin", remoteRepo.GitDir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git remote add failed: %s", string(out))

	store, err := NewGitStore(ctx, types.Format_DOLT.VersionString(), localRepo.GitDir, blobstore.DoltDataRef, blobstore.GitBlobstoreOptions{}, 0, NewUnlimitedMemQuotaProvider())
	require.NoError(t, err)
	defer store.Close()

	// Assert the underlying blobstore is a GitBlobstore and that chunked writes are enabled by default.
	bsp, ok := store.persister.(*blobstorePersister)
	require.True(t, ok, "expected persister to be *blobstorePersister, got %T", store.persister)

	gbs, ok := bsp.bs.(*blobstore.GitBlobstore)
	require.True(t, ok, "expected blobstore to be *blobstore.GitBlobstore, got %T", bsp.bs)

	// Use a totalSize larger than the default 50MB threshold, but provide a tiny reader.
	// planPutWrites decides chunked-vs-inline based on totalSize, and should pick chunked mode
	// when the default max part size is enabled.
	_, err = gbs.Put(ctx, "k", int64(50*1024*1024+1), bytes.NewReader([]byte("hi")))
	require.NoError(t, err)

	// On the remote, key "k" should be represented as a tree containing part "0001".
	cmd = exec.CommandContext(ctx, "git", "--git-dir", remoteRepo.GitDir, "rev-parse", "--verify", "--quiet", blobstore.DoltDataRef+"^{commit}")
	headBytes, err := cmd.CombinedOutput()
	require.NoError(t, err, "git rev-parse failed: %s", string(headBytes))

	commit := string(bytes.TrimSpace(headBytes))
	cmd = exec.CommandContext(ctx, "git", "--git-dir", remoteRepo.GitDir, "ls-tree", commit, "k")
	lsOut, err := cmd.CombinedOutput()
	require.NoError(t, err, "git ls-tree failed: %s", string(lsOut))
	require.Contains(t, string(lsOut), "\tk\n")
	require.Contains(t, string(lsOut), "tree")

	cmd = exec.CommandContext(ctx, "git", "--git-dir", remoteRepo.GitDir, "ls-tree", commit, "k/0001")
	partOut, err := cmd.CombinedOutput()
	require.NoError(t, err, "git ls-tree part failed: %s", string(partOut))
	require.Contains(t, string(partOut), "0001")
}
