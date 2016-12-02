// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestCompactMemTable(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	tableAddr, chunkCount := compact(dir, mt, nil)
	if assert.True(chunkCount > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, tableAddr.String()))
		assert.NoError(err)
		tr := newTableReader(buff, memReaderAt(buff))
		for _, c := range chunks {
			assert.True(tr.has(computeAddr(c)))
		}
	}
}

func TestCompactMemTableNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(1024)
	existingTable := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.True(mt.addChunk(computeAddr(c), c))
		assert.True(existingTable.addChunk(computeAddr(c), c))
	}

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	tableAddr, chunkCount := compact(dir, mt, existingTable)
	assert.True(chunkCount == 0)

	_, err = os.Stat(filepath.Join(dir, tableAddr.String()))
	assert.True(os.IsNotExist(err), "%v", err)
}
