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

package dbfactory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

// t.TempDir() includes the test name on disk, which can create very long paths on Windows.
// These tests create deep `refs/...` paths inside bare git repos and can hit MAX_PATH without
// long path support enabled. Use a short temp prefix on Windows to keep paths under the limit.
func shortTempDir(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "windows" {
		return t.TempDir()
	}

	dir, err := os.MkdirTemp("", "dolt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestGitRemoteURLString(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		expected string
	}{
		{
			name:     "ssh relative path reconstructs SCP-style",
			rawURL:   "ssh://git@myhost/./relative/repo.git",
			expected: "git@myhost:relative/repo.git",
		},
		{
			name:     "ssh absolute path unchanged",
			rawURL:   "ssh://git@myhost/abs/repo.git",
			expected: "ssh://git@myhost/abs/repo.git",
		},
		{
			name:     "https unchanged",
			rawURL:   "https://example.com/org/repo.git",
			expected: "https://example.com/org/repo.git",
		},
		{
			name:     "ssh no user relative path",
			rawURL:   "ssh://myhost/./relative/repo.git",
			expected: "myhost:relative/repo.git",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.rawURL)
			require.NoError(t, err)
			got := gitRemoteURLString(u)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestGitRemoteFactory_GitFile_RequiresGitCacheRootParam(t *testing.T) {
	ctx := context.Background()
	_, _, _, err := CreateDB(ctx, types.Format_DOLT, "git+file:///tmp/remote.git", map[string]interface{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), GitCacheRootParam)
}

func TestGitRemoteFactory_GitFile_CachesUnderRepoDoltDirAndCanWrite(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	localRepoRoot := shortTempDir(t)

	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	remoteURL := "file://" + remotePath
	urlStr := "git+file://" + remotePath
	// The Dolt CLI stores caches under <repoRoot>/.dolt/git-remote-cache; the
	// factory uses git_cache_root verbatim, so the caller composes that path.
	cacheBase := filepath.Join(localRepoRoot, DoltDir, GitRemoteCacheDirName)
	params := map[string]interface{}{
		GitCacheRootParam: cacheBase,
	}

	db, vrw, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.NotNil(t, vrw)

	sum := sha256.Sum256([]byte(remoteURL + "|" + "refs/dolt/data"))
	h := hex.EncodeToString(sum[:])
	cacheRepo := filepath.Join(cacheBase, h, "repo.git")
	_, err = os.Stat(filepath.Join(cacheRepo, "HEAD"))
	require.NoError(t, err)

	vs, ok := vrw.(*types.ValueStore)
	require.True(t, ok, "expected ValueReadWriter to be *types.ValueStore, got %T", vrw)
	cs := vs.ChunkStore()

	// Minimal write: put one chunk and commit its hash as the root.
	c := chunks.NewChunk([]byte("hello\n"))
	err = cs.Put(ctx, c, func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	})
	require.NoError(t, err)

	last, err := cs.Root(ctx)
	require.NoError(t, err)
	okCommit, err := cs.Commit(ctx, c.Hash(), last)
	require.NoError(t, err)
	require.True(t, okCommit)

	require.NoError(t, db.Close())

	// Remote should now have refs/dolt/data.
	cmd := exec.CommandContext(ctx, "git", "--git-dir", remoteRepo.GitDir, "rev-parse", "--verify", "--quiet", "refs/dolt/data^{commit}")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git rev-parse failed: %s", strings.TrimSpace(string(out)))
}

func TestGitRemoteFactory_TwoClientsDistinctCacheDirsRoundtrip(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	// Extend the default 1s syncForRead TTL to 5s to account for slow CI machines.
	prevTTL := gitBlobstoreSyncForReadTTLOverride
	gitBlobstoreSyncForReadTTLOverride = 5 * time.Second
	t.Cleanup(func() { gitBlobstoreSyncForReadTTLOverride = prevTTL })

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	urlStr := "git+file://" + remotePath

	noopGetAddrs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}

	open := func(cacheRoot string) (db datas.Database, cs chunks.ChunkStore) {
		params := map[string]interface{}{
			GitCacheRootParam: cacheRoot,
		}
		d, vrw, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
		require.NoError(t, err)
		require.NotNil(t, d)
		require.NotNil(t, vrw)

		vs, ok := vrw.(*types.ValueStore)
		require.True(t, ok, "expected ValueReadWriter to be *types.ValueStore, got %T", vrw)
		return d, vs.ChunkStore()
	}

	cacheA := shortTempDir(t)
	cacheB := shortTempDir(t)

	// Client A writes a root pointing at chunk A.
	dbA, csA := open(cacheA)
	cA := chunks.NewChunk([]byte("clientA\n"))
	require.NoError(t, csA.Put(ctx, cA, noopGetAddrs))
	lastA, err := csA.Root(ctx)
	require.NoError(t, err)
	okCommitA, err := csA.Commit(ctx, cA.Hash(), lastA)
	require.NoError(t, err)
	require.True(t, okCommitA)
	require.NoError(t, dbA.Close())

	// Client B reads chunk A, then writes chunk B and updates the root.
	dbB, csB := open(cacheB)
	require.NoError(t, csB.Rebase(ctx))
	rootB, err := csB.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cA.Hash(), rootB)
	gotA, err := csB.Get(ctx, cA.Hash())
	require.NoError(t, err)
	require.Equal(t, "clientA\n", string(gotA.Data()))

	cB := chunks.NewChunk([]byte("clientB\n"))
	require.NoError(t, csB.Put(ctx, cB, noopGetAddrs))
	okCommitB, err := csB.Commit(ctx, cB.Hash(), rootB)
	require.NoError(t, err)
	require.True(t, okCommitB)
	require.NoError(t, dbB.Close())

	// Cached blobstore: Rebase within syncForRead TTL skips upstream fetch.
	dbA2, csA2 := open(cacheA)
	require.NoError(t, csA2.Rebase(ctx))
	rootA2, err := csA2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cA.Hash(), rootA2)

	time.Sleep(10 * time.Second)
	require.NoError(t, csA2.Rebase(ctx))
	rootA2After, err := csA2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cB.Hash(), rootA2After)
	gotB, err := csA2.Get(ctx, cB.Hash())
	require.NoError(t, err)
	require.Equal(t, "clientB\n", string(gotB.Data()))
	require.NoError(t, dbA2.Close())
}

func TestGitRemoteFactory_GitFile_RemoteWithNoBranchesFails(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)

	localRepoRoot := shortTempDir(t)
	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	urlStr := "git+file://" + remotePath
	params := map[string]interface{}{
		GitCacheRootParam: localRepoRoot,
	}

	_, _, _, err = CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrGitRemoteHasNoBranches)
}

func TestEnsureGitRemoteURL_IdempotentRemoteAlreadyExists(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	gitDir := filepath.Join(shortTempDir(t), "repo.git")

	// Create a real bare git repo.
	out, err := exec.CommandContext(ctx, "git", "init", "--bare", gitDir).CombinedOutput()
	require.NoError(t, err, "git init --bare failed: %s", string(out))

	remoteName := "origin"
	remoteURL := "https://example.com/repo.git"

	// First call: adds the remote.
	require.NoError(t, ensureGitRemoteURL(ctx, gitDir, remoteName, remoteURL))

	// Second call: remote already exists, falls back to set-url.
	require.NoError(t, ensureGitRemoteURL(ctx, gitDir, remoteName, remoteURL))

	// Verify the remote URL is correct.
	got, err := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "remote", "get-url", remoteName).CombinedOutput()
	require.NoError(t, err, "git remote get-url failed: %s", string(got))
	require.Equal(t, remoteURL, strings.TrimSpace(string(got)))
}

func TestCloseGitRemotesUnderRoot(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	t.Cleanup(func() { TeardownGitRemotes(ctx) })

	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	urlStr := "git+file://" + filepath.ToSlash(remoteRepo.GitDir)
	open := func(root string) datas.Database {
		db, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, map[string]interface{}{GitCacheRootParam: filepath.Join(root, DoltDir, GitRemoteCacheDirName)})
		require.NoError(t, err)
		return db
	}

	rootA, rootB := shortTempDir(t), shortTempDir(t)
	dbA, dbB := open(rootA), open(rootB)
	require.True(t, open(rootA) == dbA, "opens under the same root share one cached store")

	require.NoError(t, CloseGitRemotesUnderRoot(rootA))

	require.True(t, open(rootB) == dbB, "closing one root's remotes must leave the others cached")

	// Deleting the cache repository is what eviction is for: an entry left behind would hand the next open
	// of this remote a store backed by files that are no longer there.
	require.NoError(t, os.RemoveAll(rootA))
	require.True(t, open(rootA) != dbA, "the open after eviction must build a new store")
}

func TestGitRemoteFactory_ReopenSeesOtherCacheRootsPush(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	// Reopening returns the process-wide cached store, so it must fetch from the remote to see a push
	// made through a different cache root. A tiny TTL keeps the read-side fetch dedup from hiding that.
	prevTTL := gitBlobstoreSyncForReadTTLOverride
	gitBlobstoreSyncForReadTTLOverride = time.Nanosecond
	t.Cleanup(func() { gitBlobstoreSyncForReadTTLOverride = prevTTL })

	ctx := context.Background()
	t.Cleanup(func() { TeardownGitRemotes(ctx) })

	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)

	urlStr := "git+file://" + filepath.ToSlash(remoteRepo.GitDir)
	open := func(root string) (datas.Database, chunks.ChunkStore) {
		db, vrw, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, map[string]interface{}{GitCacheRootParam: filepath.Join(root, DoltDir, GitRemoteCacheDirName)})
		require.NoError(t, err)
		vs, ok := vrw.(*types.ValueStore)
		require.True(t, ok, "expected ValueReadWriter to be *types.ValueStore, got %T", vrw)
		return db, vs.ChunkStore()
	}
	noopGetAddrs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}

	rootA, rootB := shortTempDir(t), shortTempDir(t)

	dbA, csA := open(rootA)
	cA := chunks.NewChunk([]byte("cacheRootA\n"))
	require.NoError(t, csA.Put(ctx, cA, noopGetAddrs))
	lastA, err := csA.Root(ctx)
	require.NoError(t, err)
	okCommit, err := csA.Commit(ctx, cA.Hash(), lastA)
	require.NoError(t, err)
	require.True(t, okCommit)

	// Cache root B pushes a new root on top of A's.
	_, csB := open(rootB)
	rootHash, err := csB.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cA.Hash(), rootHash)
	cB := chunks.NewChunk([]byte("cacheRootB\n"))
	require.NoError(t, csB.Put(ctx, cB, noopGetAddrs))
	okCommit, err = csB.Commit(ctx, cB.Hash(), rootHash)
	require.NoError(t, err)
	require.True(t, okCommit)

	dbA2, csA2 := open(rootA)
	require.True(t, dbA2 == dbA, "reopen under the same root returns the cached store")
	rootA2, err := csA2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cB.Hash(), rootA2, "reopen must see the root pushed through cache root B")
	got, err := csA2.Get(ctx, cB.Hash())
	require.NoError(t, err)
	require.Equal(t, "cacheRootB\n", string(got.Data()))
}

func TestGitRemoteFactory_GitCacheRootParamUsedVerbatim(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
	ctx := context.Background()
	t.Cleanup(func() { TeardownGitRemotes(ctx) })

	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remoteRepo.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)
	urlStr := "git+file://" + filepath.ToSlash(remoteRepo.GitDir)

	// GitCacheRootParam is used as the cache base verbatim: no .dolt/git-remote-cache
	// is appended, so a non-Dolt embedder gets a clean cache location.
	cacheDir := shortTempDir(t)
	db, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, map[string]interface{}{GitCacheRootParam: cacheDir})
	require.NoError(t, err)
	require.NotNil(t, db)

	_, statErr := os.Stat(filepath.Join(cacheDir, DoltDir))
	require.True(t, os.IsNotExist(statErr), "cache dir must not contain a .dolt directory")

	entries, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if e.IsDir() {
			if _, err := os.Stat(filepath.Join(cacheDir, e.Name(), "repo.git")); err == nil {
				found = true
			}
		}
	}
	require.True(t, found, "expected the cache repo directly under <git_cache_root>/<hash>/repo.git")
}
