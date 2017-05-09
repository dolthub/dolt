// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestFDCache(t *testing.T) {
	dir := makeTempDir(assert.New(t))
	defer os.RemoveAll(dir)

	paths := [3]string{}
	for i := range paths {
		name := fmt.Sprintf("file%d", i)
		paths[i] = filepath.Join(dir, name)
		err := ioutil.WriteFile(paths[i], []byte(name), 0644)
		assert.NoError(t, err)
	}

	refNoError := func(fc *fdCache, p string, assert *assert.Assertions) *os.File {
		f, err := fc.RefFile(p)
		assert.NoError(err)
		assert.NotNil(f)
		return f
	}

	t.Run("NoEvictions", func(t *testing.T) {
		assert := assert.New(t)
		fc := newFDCache(2)
		defer fc.Drop()
		f := refNoError(fc, paths[0], assert)

		f2 := refNoError(fc, paths[1], assert)
		assert.NotEqual(f, f2)

		dup := refNoError(fc, paths[0], assert)
		assert.Equal(f, dup)
	})

	t.Run("Evictions", func(t *testing.T) {
		assert := assert.New(t)
		fc := newFDCache(1)
		defer fc.Drop()

		f := refNoError(fc, paths[0], assert)
		f2 := refNoError(fc, paths[1], assert)
		assert.NotEqual(f, f2)

		// f wasn't evicted, because it's still reffed
		dup := refNoError(fc, paths[0], assert)
		assert.Equal(f, dup)

		expected := sort.StringSlice(paths[:2])
		sort.Sort(expected)
		assert.EqualValues(expected, fc.reportEntries())

		fc.UnrefFile(paths[0])
		fc.UnrefFile(paths[0])
		fc.UnrefFile(paths[1])

		// NOW, we should be able to evict both f and f2
		f3 := refNoError(fc, paths[2], assert)
		assert.NotEqual(f, f2)
		assert.NotEqual(f2, f3)

		assert.EqualValues(paths[2:], fc.reportEntries())
	})
}
