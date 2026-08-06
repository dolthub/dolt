// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func makeFileManifestTempDir(t *testing.T) fileManifest {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	fm, err := getFileManifest(context.Background(), dir)
	require.NoError(t, err)
	return fm.(fileManifest)
}

func TestFileManifestLoadIfExists(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	exists, upstream, err := fm.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.False(exists)

	// Simulate another process writing a manifest (with an old Noms version).
	jerk := computeAddr([]byte("jerk"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	gcGen := hash.Hash{}
	m := strings.Join([]string{StorageVersion, "0", jerk.String(), newRoot.String(), gcGen.String(), tableName.String(), "0"}, ":")
	err = clobberManifest(fm.dir, m)
	require.NoError(t, err)

	// ParseIfExists should now reflect the manifest written above.
	exists, upstream, err = fm.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal("0", upstream.nbfVers)
	assert.Equal(jerk, upstream.lock)
	assert.Equal(newRoot, upstream.root)
	if assert.Len(upstream.specs, 1) {
		assert.Equal(tableName.String(), upstream.specs[0].name.String())
		assert.Equal(uint32(0), upstream.specs[0].chunkCount)
	}
}

func TestFileManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	// Simulate another process having already put old Noms data in dir/.
	m := strings.Join([]string{StorageVersion, "0", hash.Hash{}.String(), hash.Hash{}.String(), hash.Hash{}.String()}, ":")
	err := clobberManifest(fm.dir, m)
	require.NoError(t, err)

	_, err = fm.Update(context.Background(), dherrors.FatalBehaviorError, hash.Hash{}, manifestContents{}, stats, nil)
	assert.Error(err)
}

func TestFileManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	l := computeAddr([]byte{0x01})
	upstream, err := fm.Update(context.Background(), dherrors.FatalBehaviorError, hash.Hash{}, manifestContents{nbfVers: constants.FormatDoltString, lock: l}, stats, nil)
	require.NoError(t, err)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	fm2, err := getFileManifest(context.Background(), fm.dir) // Open existent, but empty manifest
	require.NoError(t, err)
	exists, upstream, err := fm2.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	l2 := computeAddr([]byte{0x02})
	upstream, err = fm2.Update(context.Background(), dherrors.FatalBehaviorError, l, manifestContents{nbfVers: constants.FormatDoltString, lock: l2}, stats, nil)
	require.NoError(t, err)
	assert.Equal(l2, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
}

func TestFileManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	// First, test winning the race against another process.
	specA := computeAddr([]byte("a"))
	touchTableFile(t, fm.dir, specA)
	contents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("locker")),
		root:    hash.Of([]byte("new root")),
		specs:   []tableSpec{{specA, 3}},
	}
	upstream, err := fm.Update(context.Background(), dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		gcGen := hash.Hash{}
		m := strings.Join([]string{StorageVersion, constants.FormatDoltString, lock.String(), newRoot2.String(), gcGen.String()}, ":")
		b, err := tryClobberManifest(fm.dir, m)
		require.NoError(t, err, string(b))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	contents2 := manifestContents{lock: computeAddr([]byte("locker 2")), root: hash.Of([]byte("new root 2")), nbfVers: constants.FormatDoltString}
	upstream, err = fm.Update(context.Background(), dherrors.FatalBehaviorError, hash.Hash{}, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = fm.Update(context.Background(), dherrors.FatalBehaviorError, upstream.lock, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents2.lock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	gcGen := hash.Hash{}
	m := strings.Join([]string{StorageVersion, constants.FormatDoltString, jerkLock.String(), contents2.root.String(), gcGen.String(), tableName.String(), "1"}, ":")
	err = clobberManifest(fm.dir, m)
	require.NoError(t, err)

	contents3 := manifestContents{lock: computeAddr([]byte("locker 3")), root: hash.Of([]byte("new root 3")), nbfVers: constants.FormatDoltString}
	upstream, err = fm.Update(context.Background(), dherrors.FatalBehaviorError, upstream.lock, contents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Equal([]tableSpec{{tableName, 1}}, upstream.specs)
}

// tryClobberManifest simulates another process trying to access dir/manifestFileName concurrently. To avoid deadlock, it does a non-blocking lock of dir/lockFileName. If it can get the lock, it clobbers the manifest.
func tryClobberManifest(dir, contents string) ([]byte, error) {
	return runClobber(dir, contents)
}

// clobberManifest simulates another process writing dir/manifestFileName concurrently. It ignores the lock file, so it's up to the caller to ensure correctness.
func clobberManifest(dir, contents string) error {
	if err := os.WriteFile(filepath.Join(dir, lockFileName), nil, 0666); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, manifestFileName), []byte(contents), 0666)
}

func runClobber(dir, contents string) ([]byte, error) {
	_, filename, _, _ := runtime.Caller(1)
	clobber := filepath.Join(filepath.Dir(filename), "test/manifest_clobber.go")
	mkPath := func(f string) string {
		return filepath.Join(dir, f)
	}

	c := exec.Command("go", "run", clobber, mkPath(lockFileName), mkPath(manifestFileName), contents)
	return c.CombinedOutput()
}

// touchTableFile creates an empty stand-in for the table file named |h| in
// |dir|, so that a manifest update may take a dependency on it.
func touchTableFile(t *testing.T, dir string, h hash.Hash) string {
	t.Helper()
	p := filepath.Join(dir, h.String())
	require.NoError(t, os.WriteFile(p, nil, 0666))
	return p
}

// TestFileManifestUpdateRejectsMissingTableFile asserts that a manifest update
// refuses to publish a dependency on a table file that is not in the
// directory. A writer that committed such a manifest would leave the store
// broken for every other process, while continuing to read the file through
// its own already-open descriptor.
func TestFileManifestUpdateRejectsMissingTableFile(t *testing.T) {
	ctx := context.Background()
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	present := computeAddr([]byte("present"))
	absent := computeAddr([]byte("absent"))
	touchTableFile(t, fm.dir, present)

	contents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock")),
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{{present, 1}, {absent, 1}},
	}

	_, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.ErrorIs(t, err, ErrManifestSpecMissingTableFile)
	require.ErrorContains(t, err, absent.String())

	// The manifest must not have been committed.
	exists, _, err := fm.ParseIfExists(ctx, stats, nil)
	require.NoError(t, err)
	require.False(t, exists, "a rejected update must not publish a manifest")
}

// TestFileManifestUpdateAcceptsArchive asserts an archive satisfies a spec.
// A tableSpec records only the address, so both spellings must be tried.
func TestFileManifestUpdateAcceptsArchive(t *testing.T) {
	ctx := context.Background()
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	archive := computeAddr([]byte("archive"))
	require.NoError(t, os.WriteFile(filepath.Join(fm.dir, archive.String()+ArchiveFileSuffix), nil, 0666))

	contents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock")),
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{{archive, 1}},
	}

	upstream, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)
	require.Equal(t, contents.lock, upstream.lock)
}

// TestFileManifestUpdateAcceptsAppendix asserts appendix specs are checked too;
// they name table files the same way the main spec list does.
func TestFileManifestUpdateAcceptsAppendix(t *testing.T) {
	ctx := context.Background()
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	appendix := computeAddr([]byte("appendix"))
	contents := manifestContents{
		nbfVers:  constants.FormatDoltString,
		lock:     computeAddr([]byte("lock")),
		root:     hash.Of([]byte("root")),
		specs:    []tableSpec{{appendix, 1}},
		appendix: []tableSpec{{appendix, 1}},
	}

	_, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.ErrorIs(t, err, ErrManifestSpecMissingTableFile)

	touchTableFile(t, fm.dir, appendix)
	upstream, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)
	require.Equal(t, contents.lock, upstream.lock)
}

// TestFileManifestUpdateSkipsExistingSpecs asserts only newly added specs are
// checked. Re-verifying the whole set on every write would cost a stat per
// table file, and those files were vouched for when they were added.
func TestFileManifestUpdateSkipsExistingSpecs(t *testing.T) {
	ctx := context.Background()
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	first := computeAddr([]byte("first"))
	firstPath := touchTableFile(t, fm.dir, first)
	contents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock 1")),
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{{first, 1}},
	}
	upstream, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)

	// Something removes the already-referenced file out from under us. The
	// store is broken, but that is not this update's doing and not its job to
	// notice.
	require.NoError(t, os.Remove(firstPath))

	second := computeAddr([]byte("second"))
	touchTableFile(t, fm.dir, second)
	contents2 := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock 2")),
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{{first, 1}, {second, 1}},
	}
	upstream, err = fm.Update(ctx, dherrors.FatalBehaviorError, upstream.lock, contents2, stats, nil)
	require.NoError(t, err)
	require.Equal(t, contents2.lock, upstream.lock)
}

// TestFileManifestUpdateGCGenRejectsMissingTableFile asserts the GC generation
// update path is checked as well; it publishes a wholly new spec list.
func TestFileManifestUpdateGCGenRejectsMissingTableFile(t *testing.T) {
	ctx := context.Background()
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	root := hash.Of([]byte("root"))
	present := computeAddr([]byte("present"))
	touchTableFile(t, fm.dir, present)
	contents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock 1")),
		root:    root,
		specs:   []tableSpec{{present, 1}},
	}
	upstream, err := fm.Update(ctx, dherrors.FatalBehaviorError, hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)

	collected := computeAddr([]byte("collected"))
	gcContents := manifestContents{
		nbfVers: constants.FormatDoltString,
		lock:    computeAddr([]byte("lock 2")),
		root:    root,
		gcGen:   computeAddr([]byte("gcgen")),
		specs:   []tableSpec{{collected, 1}},
	}
	_, err = fm.UpdateGCGen(ctx, dherrors.FatalBehaviorError, upstream.lock, gcContents, stats, nil)
	require.ErrorIs(t, err, ErrManifestSpecMissingTableFile)

	touchTableFile(t, fm.dir, collected)
	after, err := fm.UpdateGCGen(ctx, dherrors.FatalBehaviorError, upstream.lock, gcContents, stats, nil)
	require.NoError(t, err)
	require.Equal(t, gcContents.lock, after.lock)
}
