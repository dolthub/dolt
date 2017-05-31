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
	"sync"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestFDCache(t *testing.T) {
	dir := makeTempDir(t)
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

	t.Run("ConcurrentOpen", func(t *testing.T) {
		assert := assert.New(t)
		concurrency := 3
		fc := newFDCache(3)
		defer fc.Drop()

		trigger := make(chan struct{})
		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-trigger
				fc.RefFile(paths[0])
			}()
		}
		close(trigger)
		wg.Wait()

		present := fc.reportEntries()
		if assert.Len(present, 1) {
			ce := fc.cache[present[0]]
			assert.EqualValues(concurrency, ce.refCount)
		}
	})

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

		f0 := refNoError(fc, paths[0], assert)
		f1 := refNoError(fc, paths[1], assert)
		assert.NotEqual(f0, f1)

		// f0 wasn't evicted, because that doesn't happen until UnrefFile()
		dup := refNoError(fc, paths[0], assert)
		assert.Equal(f0, dup)

		expected := sort.StringSlice(paths[:2])
		sort.Sort(expected)
		assert.EqualValues(expected, fc.reportEntries())

		// Unreffing f1 now should evict it
		fc.UnrefFile(paths[1])
		assert.EqualValues(paths[:1], fc.reportEntries())

		// Bring f1 back so we can test multiple evictions in a row
		f1 = refNoError(fc, paths[1], assert)
		assert.NotEqual(f0, f1)

		// After adding f3, we should be able to evict both f0 and f1
		f2 := refNoError(fc, paths[2], assert)
		assert.NotEqual(f0, f2)
		assert.NotEqual(f1, f2)

		fc.UnrefFile(paths[0])
		fc.UnrefFile(paths[0])
		fc.UnrefFile(paths[1])

		assert.EqualValues(paths[2:], fc.reportEntries())
	})
}
