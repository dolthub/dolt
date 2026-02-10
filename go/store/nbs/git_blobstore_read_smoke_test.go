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
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

func TestGitBlobstoreReadSmoke_ManifestAndTableAccessPatterns(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)

	// Seed a valid v5 manifest with no tables. This should allow NBS to open
	// without triggering any write paths.
	mc := manifestContents{
		nbfVers: types.Format_DOLT.VersionString(),
		lock:    hash.Of([]byte("lock")),
		root:    hash.Of([]byte("root")),
		gcGen:   hash.Of([]byte("gcgen")),
		specs:   nil,
	}
	var buf bytes.Buffer
	require.NoError(t, writeManifest(&buf, mc))

	// Seed a "table-like" blob to exercise the same access patterns NBS uses:
	// - tail reads via negative BlobRange offsets
	// - ReadAt-style ranged reads (ReadAtWithStats)
	table := make([]byte, 64*1024)
	for i := range table {
		table[i] = byte(i % 251)
	}

	commit, err := remoteRepo.SetRefToTree(ctx, blobstore.DoltDataRef, map[string][]byte{
		"manifest": buf.Bytes(),
		"table":    table,
	}, "seed refs/dolt/data for smoke test")
	require.NoError(t, err)
	require.NotEmpty(t, commit)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", localRepo.GitDir, "remote", "add", "origin", remoteRepo.GitDir)
	remoteAddOut, err := cmd.CombinedOutput()
	require.NoError(t, err, "git remote add failed: %s", string(remoteAddOut))

	bs, err := blobstore.NewGitBlobstore(localRepo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	// 1) Manifest read path via blobstoreManifest.ParseIfExists.
	stats := NewStats()
	exists, got, err := blobstoreManifest{bs: bs}.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, mc.nbfVers, got.nbfVers)
	require.Equal(t, mc.root, got.root)
	require.Equal(t, mc.lock, got.lock)
	require.Equal(t, mc.gcGen, got.gcGen)
	require.Len(t, got.specs, 0)

	// 2) Tail-read pattern used by table index/footer loads:
	//    bs.Get(key, NewBlobRange(-N, 0)) and io.ReadFull.
	const tailN = 1024
	rc, totalSz, ver, err := bs.Get(ctx, "table", blobstore.NewBlobRange(-tailN, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(len(table)), totalSz)
	require.NotEmpty(t, ver)
	tail := make([]byte, tailN)
	_, err = io.ReadFull(rc, tail)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, table[len(table)-tailN:], tail)

	// Per-key version should be stable across reads.
	rc2, _, ver2, err := bs.Get(ctx, "table", blobstore.AllRange)
	require.NoError(t, err)
	// Drain before close to avoid broken-pipe errors from killing git early.
	_, err = io.Copy(io.Discard, rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())
	require.Equal(t, ver, ver2)

	// 3) ReadAt-style ranged reads used by table readers.
	tr := &bsTableReaderAt{bs: bs, key: "table"}
	out := make([]byte, 4096)
	n, err := tr.ReadAtWithStats(ctx, out, 1234, stats)
	require.NoError(t, err)
	require.Equal(t, len(out), n)
	require.Equal(t, table[1234:1234+int64(len(out))], out)

	// Near-end reads should return short read without error.
	out2 := make([]byte, 4096)
	start := int64(len(table) - 100)
	n, err = tr.ReadAtWithStats(ctx, out2, start, stats)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, table[start:], out2[:n])
}
