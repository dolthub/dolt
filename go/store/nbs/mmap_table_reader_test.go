// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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

	tableData, h, err := buildTable(chunks)
	assert.NoError(err)
	err = ioutil.WriteFile(filepath.Join(dir, h.String()), tableData, 0666)
	assert.NoError(err)

	trc, err := newMmapTableReader(dir, h, uint32(len(chunks)), nil, fc)
	assert.NoError(err)
	assertChunksInReader(chunks, trc, assert)
}
