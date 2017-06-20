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

	"github.com/attic-labs/testify/assert"
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

		tc := newFSTableCache(dir, uint64(sum), len(datas))
		for _, d := range datas {
			tc.store(computeAddr(d), bytes.NewReader(d), uint64(len(d)))
		}

		expiredName := computeAddr(datas[0])
		assert.Nil(t, tc.checkout(expiredName))
		_, fserr := os.Stat(filepath.Join(dir, expiredName.String()))
		assert.True(t, os.IsNotExist(fserr))

		for _, d := range datas[1:] {
			name := computeAddr(d)
			r := tc.checkout(name)
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

			names := []addr{}
			for i := byte(0); i < 4; i++ {
				name := computeAddr([]byte{i})
				assert.NoError(ioutil.WriteFile(filepath.Join(dir, name.String()), nil, 0666))
				names = append(names, name)
			}
			var ftc *fsTableCache
			assert.NotPanics(func() { ftc = newFSTableCache(dir, 1024, 4) })
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
			assert.Panics(t, func() { newFSTableCache(dir, 1024, 4) })
		})

		t.Run("ClearTempFile", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)

			tempFile := filepath.Join(dir, tempTablePrefix+"boo")
			assert.NoError(t, ioutil.WriteFile(tempFile, nil, 0666))
			assert.NotPanics(t, func() { newFSTableCache(dir, 1024, 4) })
			_, fserr := os.Stat(tempFile)
			assert.True(t, os.IsNotExist(fserr))
		})

		t.Run("Dir", func(t *testing.T) {
			t.Parallel()
			dir := makeTempDir(t)
			defer os.RemoveAll(dir)
			assert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0777))
			assert.Panics(t, func() { newFSTableCache(dir, 1024, 4) })
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
