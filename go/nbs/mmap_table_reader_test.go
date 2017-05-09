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

func TestMmapTableReader(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	fc := newFDCache(1)
	defer fc.Drop()

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h := buildTable(chunks)
	err = ioutil.WriteFile(filepath.Join(dir, h.String()), tableData, 0666)
	assert.NoError(err)

	trc := newMmapTableReader(dir, h, uint32(len(chunks)), nil, fc)
	assertChunksInReader(chunks, trc, assert)
}
