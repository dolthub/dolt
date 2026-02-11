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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

func TestGitRemoteFactory_GitFile_UsesConfiguredCacheDirAndCanWrite(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	cacheDir := t.TempDir()

	remotePath := filepath.ToSlash(remoteRepo.GitDir)
	remoteURL := "file://" + remotePath
	urlStr := "git+file://" + remotePath + "?ref=refs/dolt/data"

	params := map[string]interface{}{
		GitCacheDirParam: cacheDir,
	}

	db, vrw, _, err := CreateDB(ctx, types.Format_Default, urlStr, params)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.NotNil(t, vrw)

	// Ensure cache repo created under configured cache dir.
	sum := sha256.Sum256([]byte(remoteURL + "|" + "refs/dolt/data"))
	h := hex.EncodeToString(sum[:])
	cacheRepo := filepath.Join(cacheDir, h, "repo.git")
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
