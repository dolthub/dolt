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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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

func TestGitRemoteFactory_GitFile_UsesDefaultCacheDirAndCanWrite(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)

	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	remoteURL := "file://" + remotePath
	urlStr := "git+file://" + remotePath
	params := map[string]interface{}{}

	db, vrw, _, err := CreateDB(ctx, types.Format_Default, urlStr, params)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.NotNil(t, vrw)

	// Ensure cache repo created under default cache dir.
	base, err := os.UserCacheDir()
	require.NoError(t, err)
	cacheBase := filepath.Join(base, "dolt", "git-remote-cache")

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
	err = cs.Put(ctx, c, func(chunks.Chunk) chunks.GetAddrsCb {
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

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)

	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	urlStr := "git+file://" + remotePath

	noopGetAddrs := func(chunks.Chunk) chunks.GetAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}

	open := func() (db datas.Database, cs chunks.ChunkStore) {
		params := map[string]interface{}{}
		d, vrw, _, err := CreateDB(ctx, types.Format_Default, urlStr, params)
		require.NoError(t, err)
		require.NotNil(t, d)
		require.NotNil(t, vrw)

		vs, ok := vrw.(*types.ValueStore)
		require.True(t, ok, "expected ValueReadWriter to be *types.ValueStore, got %T", vrw)
		return d, vs.ChunkStore()
	}

	// Client A writes a root pointing at chunk A.
	dbA, csA := open()
	cA := chunks.NewChunk([]byte("clientA\n"))
	require.NoError(t, csA.Put(ctx, cA, noopGetAddrs))
	lastA, err := csA.Root(ctx)
	require.NoError(t, err)
	okCommitA, err := csA.Commit(ctx, cA.Hash(), lastA)
	require.NoError(t, err)
	require.True(t, okCommitA)
	require.NoError(t, dbA.Close())

	// Client B reads chunk A, then writes chunk B and updates the root.
	dbB, csB := open()
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

	// Client A re-opens and should see B's update.
	dbA2, csA2 := open()
	require.NoError(t, csA2.Rebase(ctx))
	rootA2, err := csA2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cB.Hash(), rootA2)
	gotB, err := csA2.Get(ctx, cB.Hash())
	require.NoError(t, err)
	require.Equal(t, "clientB\n", string(gotB.Data()))
	require.NoError(t, dbA2.Close())
}
