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
	"slices"
	"testing"
	"time"

	"github.com/dolthub/fslock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
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

// backdateDir makes |dir| read as quiescent by aging every entry in it well
// past the grace period.
func backdateDir(t *testing.T, dir string) {
	t.Helper()
	aged := time.Now().Add(-2 * testGrace)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, os.Chtimes(filepath.Join(dir, e.Name()), aged, aged))
	}
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

func keepSet(hashes ...hash.Hash) hash.HashSet {
	keep := make(hash.HashSet, len(hashes))
	for _, h := range hashes {
		keep.Insert(h)
	}
	return keep
}

// keepUnlocked hands the prune a fixed keep set and a no-op release, for tests
// that are not exercising the manifest lock.
func keepUnlocked(hashes ...hash.Hash) lockKeepers {
	keep := keepSet(hashes...)
	return func(context.Context) (hash.HashSet, func() error, error) {
		return keep, func() error { return nil }, nil
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

	// Subdirectories belong to another manifest.
	require.NoError(t, os.Mkdir(filepath.Join(dir, "oldgen"), os.ModePerm))
	oldGenFile := writeAgedFile(t, filepath.Join(dir, "oldgen"), unreferenced.String(), 16, old)

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked(referenced, referencedArchive))
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

// TestPruneDirKeepsAppendixReferences covers a file reachable only through the
// manifest appendix, which getSpecSet alone does not report.
func TestPruneDirKeepsAppendixReferences(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	spec := computeAddr([]byte("spec"))
	appendix := computeAddr([]byte("appendix"))

	contents := manifestContents{
		specs:    []tableSpec{{name: spec, chunkCount: 1}},
		appendix: []tableSpec{{name: appendix, chunkCount: 1}},
	}
	referenced := hash.HashSet(contents.getSpecSet())
	for h := range contents.getAppendixSet() {
		referenced.Insert(h)
	}

	specPath := writeAgedFile(t, dir, spec.String(), 16, 2*testGrace)
	appendixPath := writeAgedFile(t, dir, appendix.String(), 16, 2*testGrace)

	now := time.Now()
	lock := func(context.Context) (hash.HashSet, func() error, error) {
		return referenced, func() error { return nil }, nil
	}
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, lock)
	require.NoError(t, err)

	assert.Equal(t, 0, stats.FilesDeleted)
	assert.True(t, exists(t, specPath))
	assert.True(t, exists(t, appendixPath))
}

// TestPruneDirQuiescenceVeto covers a recent mtime protecting every candidate
// in the directory, not just the file that carries it.
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
			stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked(referenced))
			require.NoError(t, err)

			assert.Equal(t, 0, stats.FilesDeleted)
			require.Len(t, stats.Skipped, 1)
			assert.Contains(t, stats.Skipped[0], tc.fresh)
			assert.True(t, exists(t, debris), "a veto must protect every file, not just the fresh one")
		})
	}
}

// TestPruneDirClockSkew covers a directory whose reported mtimes are nothing
// like the local clock. Its metadata is not something we understand well enough
// to delete from, and pruning is optional housekeeping, so skip it.
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
			stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now.Add(tc.skew), now, keepUnlocked())
			require.NoError(t, err)

			assert.Equal(t, 0, stats.FilesDeleted)
			require.Len(t, stats.Skipped, 1)
			assert.Contains(t, stats.Skipped[0], "local clock")
			assert.True(t, exists(t, debris))
		})
	}
}

// TestPruneDirDecidesInFilesystemTimeBase covers the cutoff being taken from the
// probe's mtime rather than the local clock, so that a local clock offset cannot
// change the outcome. The ages are chosen to discriminate: a cutoff taken from
// the local clock would spare the debris in the "local clock behind" case.
func TestPruneDirDecidesInFilesystemTimeBase(t *testing.T) {
	for _, tc := range []struct {
		name string
		skew time.Duration
	}{
		{"local clock behind the filesystem", -3 * testGrace / 4},
		{"local clock ahead of the filesystem", 3 * testGrace / 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := makeTempDir(t)
			defer file.RemoveAll(dir)

			unreferenced := computeAddr([]byte("unreferenced"))
			debris := writeAgedFile(t, dir, unreferenced.String(), 16, 3*testGrace/2)

			// The probe reads as the same moment the files were aged against;
			// only the local clock moves.
			probeMtime := time.Now()
			stats, err := pruneDirAsOf(t.Context(), dir, testGrace, probeMtime, probeMtime.Add(tc.skew), keepUnlocked())
			require.NoError(t, err)

			assert.Empty(t, stats.Skipped)
			assert.Equal(t, 1, stats.FilesDeleted)
			assert.False(t, exists(t, debris))
		})
	}
}

// TestPruneDirIgnoresFilesCreatedAfterSnapshot covers the initial scan bounding
// what can be deleted, whatever a writer lands afterward.
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
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked())
	require.NoError(t, err)

	assert.Equal(t, 1, stats.FilesDeleted)
	assert.False(t, exists(t, debris))
	assert.True(t, exists(t, latePath), "a table file landed after the snapshot must survive")
	assert.True(t, exists(t, lateTemp), "a temp file created after the snapshot must survive")
}

// TestPruneDirRestatsBeforeUnlink covers a candidate replaced after the scan.
// Table file names are content-addressed, so a writer can rename a live file
// over one we classified as garbage — and a directory that was written to after
// the scan is one whose other mtimes are stale too, so the pass stops.
func TestPruneDirRestatsBeforeUnlink(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	// Candidates are visited in directory order; the assertion is about the file
	// visited after the one that stops the pass.
	names := []string{computeAddr([]byte("one")).String(), computeAddr([]byte("two")).String()}
	slices.Sort(names)
	replaced, untouched := names[0], names[1]

	replacedPath := writeAgedFile(t, dir, replaced, 16, 2*testGrace)
	untouchedPath := writeAgedFile(t, dir, untouched, 16, 2*testGrace)

	_testPruneAfterSnapshotHook = func() {
		// Same name, different content: what a sync starting now would do.
		writeAgedFile(t, dir, replaced, 4096, 0)
	}
	defer func() { _testPruneAfterSnapshotHook = nil }()

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked())
	require.NoError(t, err)

	assert.Equal(t, 0, stats.FilesDeleted)
	require.Len(t, stats.Skipped, 1)
	assert.Contains(t, stats.Skipped[0], replaced)
	assert.True(t, exists(t, replacedPath), "a candidate replaced after the snapshot must survive")
	assert.True(t, exists(t, untouchedPath), "one changed candidate must abandon the rest of the pass")
}

// TestPruneDirProbeHandling covers probe files neither vetoing a pass nor
// accumulating: a fresh one belongs to a live pruner, an old one to a dead one.
func TestPruneDirProbeHandling(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)
	freshProbe := writeAgedFile(t, dir, pruneProbePrefix+"concurrent", 0, 0)
	staleProbe := writeAgedFile(t, dir, pruneProbePrefix+"abandoned", 0, 2*testGrace)

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked())
	require.NoError(t, err)

	assert.Empty(t, stats.Skipped, "another pruner's probe is not evidence of a writer")
	assert.Equal(t, 1, stats.FilesDeleted, "swept probes are not counted as reclaimed table files")
	assert.False(t, exists(t, debris))
	assert.True(t, exists(t, freshProbe))
	assert.False(t, exists(t, staleProbe))
}

// TestPruneDirWithGraceDropsProbe covers the probe being cleaned up on the way
// out.
func TestPruneDirWithGraceDropsProbe(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)

	stats, err := pruneDirWithGrace(t.Context(), dir, testGrace, keepUnlocked())
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesDeleted)
	assert.False(t, exists(t, debris))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the probe should have been removed")
}

// TestPruneDirSkipsIfManifestChangedSinceScan covers a manifest published
// between the scan and the unlinks, which means a sync was in flight while the
// pass was concluding that none was.
func TestPruneDirSkipsIfManifestChangedSinceScan(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	unreferenced := computeAddr([]byte("unreferenced"))
	debris := writeAgedFile(t, dir, unreferenced.String(), 16, 2*testGrace)
	writeAgedFile(t, dir, manifestFileName, 16, 2*testGrace)

	// The scan has already recorded every mtime, so this reaches only the
	// re-stat under the lock.
	_testPruneAfterSnapshotHook = func() { writeAgedFile(t, dir, manifestFileName, 24, 0) }
	defer func() { _testPruneAfterSnapshotHook = nil }()

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, keepUnlocked())
	require.NoError(t, err)

	assert.Equal(t, 0, stats.FilesDeleted)
	require.Len(t, stats.Skipped, 1)
	assert.Contains(t, stats.Skipped[0], "manifest was updated")
	assert.True(t, exists(t, debris))
}

// TestPruneDirUnlinksUnderOneLock covers a large prune deleting everything in a
// single acquisition of the manifest lock.
func TestPruneDirUnlinksUnderOneLock(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	const count = 33
	var debris []string
	for i := 0; i < count; i++ {
		h := computeAddr([]byte{byte(i)})
		debris = append(debris, writeAgedFile(t, dir, h.String(), 16, 2*testGrace))
	}

	var acquisitions int
	lock := func(context.Context) (hash.HashSet, func() error, error) {
		acquisitions++
		return keepSet(), func() error { return nil }, nil
	}

	now := time.Now()
	stats, err := pruneDirAsOf(t.Context(), dir, testGrace, now, now, lock)
	require.NoError(t, err)

	assert.Equal(t, count, stats.FilesDeleted)
	assert.Equal(t, 1, acquisitions)
	for _, p := range debris {
		assert.False(t, exists(t, p))
	}
}

// TestNBSPruneUnreferencedWithGrace covers the store-level entry point against a
// real local store, which is how a file:// backup destination is opened.
func TestNBSPruneUnreferencedWithGrace(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	// Files the manifest references.
	fileToData := populateLocalStore(t, st, 4)
	require.NotEmpty(t, fileToData)

	// What an interrupted sync leaves behind: a complete table file that no
	// manifest references.
	orphanData, orphanAddr, err := buildTable([][]byte{[]byte("orphaned chunk")})
	require.NoError(t, err)
	orphan := filepath.Join(nomsDir, orphanAddr.String())
	require.NoError(t, os.WriteFile(orphan, orphanData, 0666))

	tempFile := filepath.Join(nomsDir, tempTablePrefix+"abandoned")
	require.NoError(t, os.WriteFile(tempFile, []byte("partial upload"), 0666))

	backdateDir(t, nomsDir)

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

// TestNBSPruneUnreferencedWithGraceNotQuiescent covers a store written to
// recently giving up nothing.
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

// TestNBSPruneUnreferencedWithGraceHoldsManifestLock covers a writer racing a
// prune. It must fail its update rather than publish a manifest naming a file
// the prune deleted.
func TestNBSPruneUnreferencedWithGraceHoldsManifestLock(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	populateLocalStore(t, st, 2)

	// The state a writer is in between landing a table file and committing the
	// manifest that names it.
	orphanData, orphanAddr, err := buildTable([][]byte{[]byte("orphaned chunk")})
	require.NoError(t, err)
	orphan := filepath.Join(nomsDir, orphanAddr.String())
	require.NoError(t, os.WriteFile(orphan, orphanData, 0666))
	backdateDir(t, nomsDir)

	// A second handle has its own file lock, standing in for another process.
	other, err := getFileManifest(ctx, nomsDir)
	require.NoError(t, err)
	defer other.Close()

	ok, upstream, err := other.ParseIfExists(ctx, &Stats{}, nil)
	require.NoError(t, err)
	require.True(t, ok)

	// The update that writer is about to attempt.
	commit := manifestContents{
		nbfVers: upstream.nbfVers,
		lock:    computeAddr([]byte("next lock")),
		root:    upstream.root,
		gcGen:   upstream.gcGen,
		specs:   append(slices.Clone(upstream.specs), tableSpec{name: orphanAddr, chunkCount: 1}),
	}

	var racedErr error
	_testPruneUnderLockHook = func() {
		_, racedErr = other.Update(ctx, dherrors.FatalBehaviorError, upstream.lock, commit, &Stats{}, nil)
	}
	defer func() { _testPruneUnderLockHook = nil }()

	stats, err := st.PruneUnreferencedWithGrace(ctx, testGrace)
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesDeleted)

	require.Error(t, racedErr)
	assert.ErrorIs(t, racedErr, fslock.ErrTimeout,
		"a manifest update must not be able to land while files are being deleted")

	// Retried after the prune, the update is refused: the file it names is gone.
	_, err = other.Update(ctx, dherrors.FatalBehaviorError, upstream.lock, commit, &Stats{}, nil)
	assert.ErrorIs(t, err, ErrManifestSpecMissingTableFile)

	_, after, err := other.ParseIfExists(ctx, &Stats{}, nil)
	require.NoError(t, err)
	assert.Equal(t, upstream.lock, after.lock, "the manifest must be untouched")
}

// TestNBSPruneUnreferencedWithGraceSkipsWhenLockHeld covers a held manifest
// lock, which means a sync is in flight.
func TestNBSPruneUnreferencedWithGraceSkipsWhenLockHeld(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	populateLocalStore(t, st, 2)

	orphanData, orphanAddr, err := buildTable([][]byte{[]byte("orphaned chunk")})
	require.NoError(t, err)
	orphan := filepath.Join(nomsDir, orphanAddr.String())
	require.NoError(t, os.WriteFile(orphan, orphanData, 0666))
	backdateDir(t, nomsDir)

	held, err := fslock.New(filepath.Join(nomsDir, lockFileName))
	require.NoError(t, err)
	require.NoError(t, held.Lock())
	defer func() {
		require.NoError(t, held.Unlock())
		require.NoError(t, held.Close())
	}()

	stats, err := st.PruneUnreferencedWithGrace(ctx, testGrace)
	require.NoError(t, err, "a busy destination is a skip, not a failure")
	assert.Equal(t, 0, stats.FilesDeleted)
	require.Len(t, stats.Skipped, 1)
	assert.Contains(t, stats.Skipped[0], "manifest lock")
	assert.True(t, exists(t, orphan))
}

// TestNBSPruneUnreferencedWithGraceKeepsPending covers a file this process has
// landed but not yet referenced. No manifest vouches for it, so the persister's
// protected set must.
func TestNBSPruneUnreferencedWithGraceKeepsPending(t *testing.T) {
	ctx := context.Background()
	st, nomsDir, _ := makeTestLocalStore(t, defaultMaxTables)
	defer st.Close()

	data, addr, err := buildTable([][]byte{[]byte("pending chunk")})
	require.NoError(t, err)
	fileID := addr.String()
	pending, err := st.WriteTableFile(ctx, fileID, 0, 1, nil, func() (io.ReadCloser, uint64, error) {
		return io.NopCloser(bytes.NewReader(data)), uint64(len(data)), nil
	})
	require.NoError(t, err)
	defer pending.Close()

	backdateDir(t, nomsDir)

	stats, err := st.PruneUnreferencedWithGrace(ctx, testGrace)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.FilesDeleted)
	assert.True(t, exists(t, filepath.Join(nomsDir, fileID)))

	// And it can still be published afterward.
	require.NoError(t, st.AddTableFilesToManifest(ctx, map[string]int{fileID: 1}, noopGetAddrs))
}

// TestCopyTableFileReplacesExistingFile guards an invariant grace pruning
// depends on: landing a table file always writes it, even over an existing file
// of the same name, so it carries a fresh mtime. Adding a "skip uploading what
// the destination already has" optimization to the pull or clone path would
// break that, and a prune could then delete a file as it goes live.
func TestCopyTableFileReplacesExistingFile(t *testing.T) {
	ctx := context.Background()
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	ftp := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false).(*fsTablePersister)

	data, addr, err := buildTable([][]byte{[]byte("a chunk")})
	require.NoError(t, err)

	// An orphan left by an interrupted sync: right name, stale content.
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
