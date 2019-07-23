// Copyright 2019 Liquidata, Inc.
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
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFSTableCache(t *testing.T) {
	datas := [][]byte{[]byte("hello"), []byte("world"), []byte("goodbye")}
	sort.SliceStable(datas, func(i, j int) bool { return len(datas[i]) < len(datas[j]) })

	t.Run("ExpireLRU", func(t *testing.T) {
		t.Parallel()
		dir := makeTempDir(t)
		defer os.RemoveAll(dir)

		sum := 0
		for _, s := range datas[1:] {
			sum += len(s)
		}

		tc, err := newFSTableCache(dir, uint64(sum), len(datas))
		assert.NoError(t, err)
		for _, d := range datas {
			err := tc.store(computeAddr(d), bytes.NewReader(d), uint64(len(d)))
			assert.NoError(t, err)
		}

		expiredName := computeAddr(datas[0])
		r, err := tc.checkout(expiredName)
		assert.NoError(t, err)
		assert.Nil(t, r)
		_, fserr := os.Stat(filepath.Join(dir, expiredName.String()))
		assert.True(t, os.IsNotExist(fserr))

		for _, d := range datas[1:] {
			name := computeAddr(d)
			r, err := tc.checkout(name)
			assert.NoError(t, err)
			assert.NotNil(t, r)
			assertDataInReaderAt(t, d, r)
			_, fserr := os.Stat(filepath.Join(dir, name.String()))
			assert.False(t, os.IsNotExist(fserr))
		}
	})

	t.Run("Init", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)
			assert := assert.New(t)

			var names []addr
			for i := byte(0); i < 4; i++ {
				name := computeAddr([]byte{i})
				assert.NoError(ioutil.WriteFile(filepath.Join(dir, name.String()), nil, 0666))
				names = append(names, name)
			}

			ftc, err := newFSTableCache(dir, 1024, 4)
			assert.NoError(err)
			assert.NotNil(ftc)

			for _, name := range names {
				assert.NotNil(ftc.checkout(name))
			}
		})

		t.Run("BadFile", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)

			assert.NoError(t, ioutil.WriteFile(filepath.Join(dir, "boo"), nil, 0666))
			_, err := newFSTableCache(dir, 1024, 4)
			assert.Error(t, err)
		})

		t.Run("ClearTempFile", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)

			tempFile := filepath.Join(dir, tempTablePrefix+"boo")
			assert.NoError(t, ioutil.WriteFile(tempFile, nil, 0666))
			_, err := newFSTableCache(dir, 1024, 4)
			assert.NoError(t, err)
			_, fserr := os.Stat(tempFile)
			assert.True(t, os.IsNotExist(fserr))
		})

		t.Run("Dir", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)
			assert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0777))
			_, err := newFSTableCache(dir, 1024, 4)
			assert.Error(t, err)
		})
	})
}

func assertDataInReaderAt(t *testing.T, data []byte, r io.ReaderAt) {
	p := make([]byte, len(data))
	n, err := r.ReadAt(p, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, p)
}
