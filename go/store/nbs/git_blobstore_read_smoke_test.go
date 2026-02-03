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
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
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

	commit, err := repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"manifest": buf.Bytes(),
		"table":    table,
	}, "seed refs/dolt/data for smoke test")
	require.NoError(t, err)
	require.NotEmpty(t, commit)

	bs, err := blobstore.NewGitBlobstore(repo.GitDir, "refs/dolt/data")
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
	require.Equal(t, commit, ver)
	tail := make([]byte, tailN)
	_, err = io.ReadFull(rc, tail)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, table[len(table)-tailN:], tail)

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

