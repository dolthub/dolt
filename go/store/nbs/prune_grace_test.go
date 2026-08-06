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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
)

const testGrace = time.Hour

// writeAgedFile writes |name| into |dir| with |size| bytes of filler and backdates its mtime by |age|.
func writeAgedFile(t *testing.T, dir, name string, size int, age time.Duration) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, bytes.Repeat([]byte("x"), size), 0666))
	mt := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(p, mt, mt))
	return p
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	require.True(t, os.IsNotExist(err), "unexpected error stat'ing %s: %v", path, err)
	return false
}

func keepSet(hashes ...hash.Hash) func(hash.Hash) bool {
	keep := make(map[hash.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		keep[h] = struct{}{}
	}
	return func(h hash.Hash) bool {
		_, ok := keep[h]
		return ok
	}
}

// TestPruneDirClassification covers which names a quiescent directory gives up
// and which it holds on to.
func TestPruneDirClassification(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	referenced := computeAddr([]byte("referenced"))
	referencedArchive := computeAddr([]byte("referenced archive"))
	unreferenced := computeAddr([]byte("unreferenced"))
	unreferencedArchive := computeAddr([]byte("unreferenced archive"))

	const old = 2 * testGrace
	kept := []string{
		writeAgedFile(t, dir, referenced.String(), 16, old),
		writeAgedFile(t, dir, referencedArchive.String()+ArchiveFileSuffix, 16, old),
		writeAgedFile(t, dir, manifestFileName, 16, old),
		writeAgedFile(t, dir, lockFileName, 0, old),
		writeAgedFile(t, dir, chunkJournalName, 16, old),
		// Unrecognized names are left alone whether or not they look
		// vaguely like ours.
		writeAgedFile(t, dir, "notes.txt", 16, old),
		// 32 characters, but not a base32 address.
		writeAgedFile(t, dir, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", 16, old),
		writeAgedFile(t, dir, referenced.String()+".bak", 16, old),
	}
	deleted := []string{
		writeAgedFile(t, dir, unreferenced.String(), 16, old),
		writeAgedFile(t, dir, unreferencedArchive.String()+ArchiveFileSuffix, 32, old),
		writeAgedFile(t, dir, tempTablePrefix+"1234", 64, old),
		writeAgedFile(t, dir, tempManifestPrefix+"5678", 8, old),
	}

	// A subdirectory is another manifest's business, and its contents are
	// invisible to this pass.
	require.NoError(t, os.Mkdir(filepath.Join(dir, "oldgen"), os.ModePerm))
	oldGenFile := writeAgedFile(t, filepath.Join(dir, "oldgen"), unreferenced.String(), 16, old)

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, keepSet(referenced, referencedArchive), testGrace, now, now)
	require.NoError(t, err)

	assert.Empty(t, stats.Skipped)
	assert.Equal(t, len(deleted), stats.FilesDeleted)
	assert.Equal(t, int64(16+32+64+8), stats.BytesReclaimed)

	for _, p := range kept {
		assert.True(t, exists(t, p), "%s should have been kept", filepath.Base(p))
	}
	for _, p := range deleted {
		assert.False(t, exists(t, p), "%s should have been deleted", filepath.Base(p))
	}
	assert.True(t, exists(t, oldGenFile), "files in subdirectories should not be touched")
}

// TestPruneDirKeepsAppendixReferences asserts that a file reachable only
// through the manifest appendix is not mistaken for garbage. The keep
// predicate is what carries that, so this pins the contract the store side
// relies on.
func TestPruneDirKeepsAppendixReferences(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	spec := computeAddr([]byte("spec"))
	appendix := computeAddr([]byte("appendix"))

	contents := manifestContents{
		specs:    []tableSpec{{name: spec, chunkCount: 1}},
		appendix: []tableSpec{{name: appendix, chunkCount: 1}},
	}
	referenced := contents.getSpecSet()
	for h := range contents.getAppendixSet() {
		referenced[h] = struct{}{}
	}

	specPath := writeAgedFile(t, dir, spec.String(), 16, 2*testGrace)
	appendixPath := writeAgedFile(t, dir, appendix.String(), 16, 2*testGrace)

	now := time.Now()
	keep := func(h hash.Hash) bool { _, ok := referenced[h]; return ok }
	stats, err := pruneDirAsOf(t.Context(), dir, keep, testGrace, now, now)
	require.NoError(t, err)

	assert.Equal(t, 0, stats.FilesDeleted)
	assert.True(t, exists(t, specPath))
	assert.True(t, exists(t, appendixPath))
}

// TestPruneDirQuiescenceVeto asserts that one recently modified file stops the
// entire pass, including deletion of unrelated debris. Partial pruning would
// reintroduce the per-file age reasoning the design rejects.
func TestPruneDirQuiescenceVeto(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fresh string
	}{
		{"referenced table file", computeAddr([]byte("referenced")).String()},
		{"temp file", tempTablePrefix + "inflight"},
		{"manifest", manifestFileName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := makeTempDir(t)
			defer file.RemoveAll(dir)

			referenced := computeAddr([]byte("referenced"))
			unreferenced := computeAddr([]byte("unreferenced"))

			writeAgedFile(t, dir, referenced.String(), 16, 2*testGrace)
			debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)
			// One minute inside the grace period is enough.
			writeAgedFile(t, dir, tc.fresh, 16, testGrace-time.Minute)

			now := time.Now()
			stats, err := pruneDirAsOf(t.Context(), dir, keepSet(referenced), testGrace, now, now)
			require.NoError(t, err)

			assert.Equal(t, 0, stats.FilesDeleted)
			require.Len(t, stats.Skipped, 1)
			assert.Contains(t, stats.Skipped[0], tc.fresh)
			assert.True(t, exists(t, debris), "a veto must protect every file, not just the fresh one")
		})
	}
}

// TestPruneDirClockSkew asserts that a directory whose filesystem clock is far
// from ours is refused rather than pruned on mtimes we cannot interpret. The
// dangerous direction is a local clock ahead of the file server, which makes
// every file look older than it is.
func TestPruneDirClockSkew(t *testing.T) {
	for _, tc := range []struct {
		name string
		skew time.Duration
	}{
		{"filesystem behind local clock", -2 * testGrace},
		{"filesystem ahead of local clock", 2 * testGrace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := makeTempDir(t)
			defer file.RemoveAll(dir)

			unreferenced := computeAddr([]byte("unreferenced"))
			debris := writeAgedFile(t, dir, unreferenced.String(), 16, 10*testGrace)

			now := time.Now()
			stats, err := pruneDirAsOf(t.Context(), dir, keepSet(), testGrace, now.Add(tc.skew), now)
			require.NoError(t, err)

			assert.Equal(t, 0, stats.FilesDeleted)
			require.Len(t, stats.Skipped, 1)
			assert.Contains(t, stats.Skipped[0], "clock")
			assert.True(t, exists(t, debris))
		})
	}
}

// TestPruneDirIgnoresFilesCreatedAfterSnapshot asserts that the candidate
// snapshot bounds what can be deleted: a writer that starts a sync after the
// quiescence check passes lands files this pass never considered.
func TestPruneDirIgnoresFilesCreatedAfterSnapshot(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)

	late := computeAddr([]byte("landed late"))
	var latePath, lateTemp string
	_testPruneAfterSnapshotHook = func() {
		latePath = writeAgedFile(t, dir, late.String(), 16, 0)
		lateTemp = writeAgedFile(t, dir, tempTablePrefix+"late", 16, 0)
	}
	defer func() { _testPruneAfterSnapshotHook = nil }()

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, keepSet(), testGrace, now, now)
	require.NoError(t, err)

	assert.Equal(t, 1, stats.FilesDeleted)
	assert.False(t, exists(t, debris))
	assert.True(t, exists(t, latePath), "a table file landed after the snapshot must survive")
	assert.True(t, exists(t, lateTemp), "a temp file created after the snapshot must survive")
}

// TestPruneDirRestatsBeforeUnlink asserts that a candidate replaced between the
// snapshot and the unlink is left alone. Names are content-addressed, so a
// writer can rename a live file over a name we had classified as garbage.
func TestPruneDirRestatsBeforeUnlink(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	replaced := computeAddr([]byte("replaced"))
	stale := computeAddr([]byte("stale"))
	replacedPath := writeAgedFile(t, dir, replaced.String(), 16, 2*testGrace)
	stalePath := writeAgedFile(t, dir, stale.String(), 16, 2*testGrace)

	_testPruneAfterSnapshotHook = func() {
		// Same name, different content: what a sync starting now would do.
		writeAgedFile(t, dir, replaced.String(), 4096, 0)
	}
	defer func() { _testPruneAfterSnapshotHook = nil }()

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, keepSet(), testGrace, now, now)
	require.NoError(t, err)

	assert.Equal(t, 1, stats.FilesDeleted)
	assert.True(t, exists(t, replacedPath), "a candidate replaced after the snapshot must survive")
	assert.False(t, exists(t, stalePath))
}

// TestPruneDirProbeHandling asserts that probe files neither veto a pass nor
// accumulate: a fresh one belongs to a live pruner, an old one to a dead one.
func TestPruneDirProbeHandling(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)
	freshProbe := writeAgedFile(t, dir, pruneProbePrefix+"concurrent", 0, 0)
	staleProbe := writeAgedFile(t, dir, pruneProbePrefix+"abandoned", 0, 2*testGrace)

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, keepSet(), testGrace, now, now)
	require.NoError(t, err)

	assert.Empty(t, stats.Skipped, "another pruner's probe is not evidence of a writer")
	assert.Equal(t, 1, stats.FilesDeleted, "swept probes are not counted as reclaimed table files")
	assert.False(t, exists(t, debris))
	assert.True(t, exists(t, freshProbe))
	assert.False(t, exists(t, staleProbe))
}

// TestPruneDirWithGraceDropsProbe asserts the end-to-end entry point creates a
// probe, uses it, and cleans it up.
func TestPruneDirWithGraceDropsProbe(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)

	stats, err := pruneDirWithGrace(t.Context(), dir, keepSet(), testGrace)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesDeleted)
	assert.False(t, exists(t, debris))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the probe should have been removed")
}

// TestNBSPruneUnreferencedWithGrace exercises the store-level entry point
// against a real local store, which is how a file:// backup destination is
// opened.
func TestNBSPruneUnreferencedWithGrace(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	// Files the manifest references.
	fileToData := populateLocalStore(t, st, 4)
	require.NotEmpty(t, fileToData)

	// A complete table file that no manifest references — what an
	// interrupted sync leaves behind.
	orphanData, orphanAddr, err := buildTable([][]byte{[]byte("orphaned chunk")})
	require.NoError(t, err)
	orphan := filepath.Join(nomsDir, orphanAddr.String())
	require.NoError(t, os.WriteFile(orphan, orphanData, 0666))

	tempFile := filepath.Join(nomsDir, tempTablePrefix+"abandoned")
	require.NoError(t, os.WriteFile(tempFile, []byte("partial upload"), 0666))

	// Backdate everything so the directory reads as quiescent.
	aged := time.Now().Add(-2 * testGrace)
	entries, err := os.ReadDir(nomsDir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, os.Chtimes(filepath.Join(nomsDir, e.Name()), aged, aged))
	}

	stats, err := st.PruneUnreferencedWithGrace(ctx, testGrace)
	require.NoError(t, err)
	assert.Empty(t, stats.Skipped)
	assert.Equal(t, 2, stats.FilesDeleted)
	assert.Equal(t, int64(len(orphanData)+len("partial upload")), stats.BytesReclaimed)

	assert.False(t, exists(t, orphan))
	assert.False(t, exists(t, tempFile))
	assert.True(t, exists(t, filepath.Join(nomsDir, manifestFileName)))
	for fileID := range fileToData {
		assert.True(t, exists(t, filepath.Join(nomsDir, fileID)), "manifest-referenced %s was pruned", fileID)
	}

	// The store must still work afterward.
	sources, err := st.Sources(ctx)
	require.NoError(t, err)
	assert.Len(t, sources.TableFiles, len(fileToData))
}

// TestNBSPruneUnreferencedWithGraceNotQuiescent asserts a store that was
// written to recently gives up nothing.
func TestNBSPruneUnreferencedWithGraceNotQuiescent(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	populateLocalStore(t, st, 2)

	orphanData, orphanAddr, err := buildTable([][]byte{[]byte("orphaned chunk")})
	require.NoError(t, err)
	orphan := filepath.Join(nomsDir, orphanAddr.String())
	require.NoError(t, os.WriteFile(orphan, orphanData, 0666))

	stats, err := st.PruneUnreferencedWithGrace(ctx, testGrace)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.FilesDeleted)
	assert.NotEmpty(t, stats.Skipped)
	assert.True(t, exists(t, orphan))
}

// TestCopyTableFileReplacesExistingFile guards the invariant that grace
// pruning depends on: a writer landing a table file in a destination always
// writes it, even when a file of that name is already there, so the file
// carries a fresh mtime.
//
// If "skip uploading a table file the destination already has" is ever added
// to the pull or clone path, that optimization must refresh the adopted file's
// mtime — otherwise a prune can delete a file at the moment it becomes live.
// This test is the tripwire.
func TestCopyTableFileReplacesExistingFile(t *testing.T) {
	ctx := context.Background()
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	ftp := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false).(*fsTablePersister)

	data, addr, err := buildTable([][]byte{[]byte("a chunk")})
	require.NoError(t, err)

	// Stand in for an orphan left by an interrupted sync: right name, stale
	// content, old mtime.
	p := writeAgedFile(t, dir, addr.String(), 7, 2*testGrace)
	before, err := os.Stat(p)
	require.NoError(t, err)

	closer, err := ftp.CopyTableFile(ctx, bytes.NewReader(data), addr.String(), 0, uint64(len(data)))
	require.NoError(t, err)
	require.NoError(t, closer.Close())

	after, err := os.Stat(p)
	require.NoError(t, err)
	assert.True(t, after.ModTime().After(before.ModTime()),
		"landing a table file over an existing name must refresh its mtime")

	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, data, got, "the existing file must be replaced, not adopted")
}
