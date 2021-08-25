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

	"github.com/dolthub/dolt/go/libraries/utils/file"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFDCache(t *testing.T) {
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	paths := [3]string{}
	for i := range paths {
		name := fmt.Sprintf("file%d", i)
		paths[i] = filepath.Join(dir, name)
		err := ioutil.WriteFile(paths[i], []byte(name), 0644)
		require.NoError(t, err)
	}

	refNoError := func(fc *fdCache, p string, assert *assert.Assertions) *os.File {
		f, err := fc.RefFile(p)
		require.NoError(t, err)
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
		err := fc.UnrefFile(paths[1])
		require.NoError(t, err)
		assert.EqualValues(paths[:1], fc.reportEntries())

		// Bring f1 back so we can test multiple evictions in a row
		f1 = refNoError(fc, paths[1], assert)
		assert.NotEqual(f0, f1)

		// After adding f3, we should be able to evict both f0 and f1
		f2 := refNoError(fc, paths[2], assert)
		assert.NotEqual(f0, f2)
		assert.NotEqual(f1, f2)

		err = fc.UnrefFile(paths[0])
		require.NoError(t, err)
		err = fc.UnrefFile(paths[0])
		require.NoError(t, err)
		err = fc.UnrefFile(paths[1])
		require.NoError(t, err)

		assert.EqualValues(paths[2:], fc.reportEntries())
	})
}
