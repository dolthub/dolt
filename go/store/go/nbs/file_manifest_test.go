// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

func makeFileManifestTempDir(t *testing.T) fileManifest {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	return fileManifest{dir: dir} //, cache: newManifestCache(defaultManifestCacheSize)}
}

func TestFileManifestLoadIfExists(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	exists, upstream := fm.ParseIfExists(context.Background(), stats, nil)
	assert.False(exists)

	// Simulate another process writing a manifest (with an old Noms version).
	jerk := computeAddr([]byte("jerk"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	err := clobberManifest(fm.dir, strings.Join([]string{StorageVersion, "0", jerk.String(), newRoot.String(), tableName.String(), "0"}, ":"))
	assert.NoError(err)

	// ParseIfExists should now reflect the manifest written above.
	exists, upstream = fm.ParseIfExists(context.Background(), stats, nil)
	assert.True(exists)
	assert.Equal("0", upstream.vers)
	assert.Equal(jerk, upstream.lock)
	assert.Equal(newRoot, upstream.root)
	if assert.Len(upstream.specs, 1) {
		assert.Equal(tableName.String(), upstream.specs[0].name.String())
		assert.Equal(uint32(0), upstream.specs[0].chunkCount)
	}
}

func TestFileManifestLoadIfExistsHoldsLock(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	// Simulate another process writing a manifest.
	lock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	err := clobberManifest(fm.dir, strings.Join([]string{StorageVersion, constants.NomsVersion, lock.String(), newRoot.String(), tableName.String(), "0"}, ":"))
	assert.NoError(err)

	// ParseIfExists should now reflect the manifest written above.
	exists, upstream := fm.ParseIfExists(context.Background(), stats, func() {
		// This should fail to get the lock, and therefore _not_ clobber the manifest.
		lock := computeAddr([]byte("newlock"))
		badRoot := hash.Of([]byte("bad root"))
		b, err := tryClobberManifest(fm.dir, strings.Join([]string{StorageVersion, "0", lock.String(), badRoot.String(), tableName.String(), "0"}, ":"))
		assert.NoError(err, string(b))
	})

	assert.True(exists)
	assert.Equal(constants.NomsVersion, upstream.vers)
	assert.Equal(newRoot, upstream.root)
	if assert.Len(upstream.specs, 1) {
		assert.Equal(tableName.String(), upstream.specs[0].name.String())
		assert.Equal(uint32(0), upstream.specs[0].chunkCount)
	}
}

func TestFileManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	// Simulate another process having already put old Noms data in dir/.
	err := clobberManifest(fm.dir, strings.Join([]string{StorageVersion, "0", addr{}.String(), hash.Hash{}.String()}, ":"))
	assert.NoError(err)

	assert.Panics(func() { fm.Update(context.Background(), addr{}, manifestContents{}, stats, nil) })
}

func TestFileManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	l := computeAddr([]byte{0x01})
	upstream := fm.Update(context.Background(), addr{}, manifestContents{vers: constants.NomsVersion, lock: l}, stats, nil)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	fm2 := fileManifest{fm.dir} // Open existent, but empty manifest
	exists, upstream := fm2.ParseIfExists(context.Background(), stats, nil)
	assert.True(exists)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	l2 := computeAddr([]byte{0x02})
	upstream = fm2.Update(context.Background(), l, manifestContents{vers: constants.NomsVersion, lock: l2}, stats, nil)
	assert.Equal(l2, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
}

func TestFileManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	// First, test winning the race against another process.
	contents := manifestContents{
		vers:  constants.NomsVersion,
		lock:  computeAddr([]byte("locker")),
		root:  hash.Of([]byte("new root")),
		specs: []tableSpec{{computeAddr([]byte("a")), 3}},
	}
	upstream := fm.Update(context.Background(), addr{}, contents, stats, func() {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		b, err := tryClobberManifest(fm.dir, strings.Join([]string{StorageVersion, constants.NomsVersion, lock.String(), newRoot2.String()}, ":"))
		assert.NoError(err, string(b))
	})
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	contents2 := manifestContents{lock: computeAddr([]byte("locker 2")), root: hash.Of([]byte("new root 2"))}
	upstream = fm.Update(context.Background(), addr{}, contents2, stats, nil)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream = fm.Update(context.Background(), upstream.lock, contents2, stats, nil)
	assert.Equal(contents2.lock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	err := clobberManifest(fm.dir, strings.Join([]string{StorageVersion, constants.NomsVersion, jerkLock.String(), contents2.root.String(), tableName.String(), "1"}, ":"))
	assert.NoError(err)

	contents3 := manifestContents{lock: computeAddr([]byte("locker 3")), root: hash.Of([]byte("new root 3"))}
	upstream = fm.Update(context.Background(), upstream.lock, contents3, stats, nil)
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
	if err := ioutil.WriteFile(filepath.Join(dir, lockFileName), nil, 0666); err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(dir, manifestFileName), []byte(contents), 0666)
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
