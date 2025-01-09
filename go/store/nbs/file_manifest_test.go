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

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func makeFileManifestTempDir(t *testing.T) fileManifest {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	fm, err := getFileManifest(context.Background(), dir, asyncFlush)
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
		assert.Equal(tableName.String(), upstream.specs[0].hash.String())
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

	_, err = fm.Update(context.Background(), hash.Hash{}, manifestContents{}, stats, nil)
	assert.Error(err)
}

func TestFileManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer file.RemoveAll(fm.dir)
	stats := &Stats{}

	l := computeAddr([]byte{0x01})
	upstream, err := fm.Update(context.Background(), hash.Hash{}, manifestContents{nbfVers: constants.FormatLD1String, lock: l}, stats, nil)
	require.NoError(t, err)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	fm2, err := getFileManifest(context.Background(), fm.dir, asyncFlush) // Open existent, but empty manifest
	require.NoError(t, err)
	exists, upstream, err := fm2.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	l2 := computeAddr([]byte{0x02})
	upstream, err = fm2.Update(context.Background(), l, manifestContents{nbfVers: constants.FormatLD1String, lock: l2}, stats, nil)
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
	contents := manifestContents{
		nbfVers: constants.FormatLD1String,
		lock:    computeAddr([]byte("locker")),
		root:    hash.Of([]byte("new root")),
		specs:   []tableSpec{{typeNoms, computeAddr([]byte("a")), 3}},
	}
	upstream, err := fm.Update(context.Background(), hash.Hash{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		gcGen := hash.Hash{}
		m := strings.Join([]string{StorageVersion, constants.FormatLD1String, lock.String(), newRoot2.String(), gcGen.String()}, ":")
		b, err := tryClobberManifest(fm.dir, m)
		require.NoError(t, err, string(b))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	contents2 := manifestContents{lock: computeAddr([]byte("locker 2")), root: hash.Of([]byte("new root 2")), nbfVers: constants.FormatLD1String}
	upstream, err = fm.Update(context.Background(), hash.Hash{}, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = fm.Update(context.Background(), upstream.lock, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents2.lock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	gcGen := hash.Hash{}
	m := strings.Join([]string{StorageVersion, constants.FormatLD1String, jerkLock.String(), contents2.root.String(), gcGen.String(), tableName.String(), "1"}, ":")
	err = clobberManifest(fm.dir, m)
	require.NoError(t, err)

	contents3 := manifestContents{lock: computeAddr([]byte("locker 3")), root: hash.Of([]byte("new root 3")), nbfVers: constants.FormatLD1String}
	upstream, err = fm.Update(context.Background(), upstream.lock, contents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Equal([]tableSpec{{typeNoms, tableName, 1}}, upstream.specs)
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
