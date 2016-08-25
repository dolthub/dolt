// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func TestRW(t *testing.T) {
	suite.Run(t, &fuseTestSuite{})
}

func (s *fuseTestSuite) TestSimpleFile() {
	datasetName := "TestSimpleFile"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	file, code := testfs.Create("coconut", uint32(os.O_CREATE|os.O_RDWR), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)
	n, code := file.Write([]byte("Lime!"), 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), uint32(5), n)
	assertAttr(s, testfs, "coconut", 0644|fuse.S_IFREG, 5)

	data := make([]byte, 5)
	rr, code := file.Read(data, 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), 5, rr.Size())
	assert.Equal(s.T(), "Lime!", string(data))

	code = testfs.Truncate("coconut", 4, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "coconut", 0644|fuse.S_IFREG, 4)
	rr, code = file.Read(data, 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), 4, rr.Size())
	assert.Equal(s.T(), "Lime!", string(data))
}

func (s *fuseTestSuite) TestBigFile() {
	datasetName := "TestBigFile"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	size := uint64(10 * 1024) // 10KB
	data := make([]byte, size)
	buf := bytes.NewBuffer(data)

	for uint64(buf.Len()) < size {
		buf.WriteString("All work and no play makes Jack a dull boy.\n")
	}

	file, code := testfs.Create("shining.txt", uint32(os.O_CREATE|os.O_RDWR), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)

	n, code := file.Write(buf.Bytes(), 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), uint32(size), n)
	assertAttr(s, testfs, "shining.txt", 0644|fuse.S_IFREG, size)
}

func (s *fuseTestSuite) TestOverwrite() {
	datasetName := "TestOverwrite"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	file, code := testfs.Create("proclamation", uint32(os.O_CREATE|os.O_RDWR), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)

	n, code := file.Write([]byte("Four score and seven years ago..."), 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), uint32(33), n)
	assertAttr(s, testfs, "proclamation", 0644|fuse.S_IFREG, 33)
	n, code = file.Write([]byte("beers"), 21)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), uint32(5), n)
	assertAttr(s, testfs, "proclamation", 0644|fuse.S_IFREG, 33)

	data := make([]byte, 33)
	rr, code := file.Read(data, 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), 33, rr.Size())
	assert.Equal(s.T(), "Four score and seven beers ago...", string(data))
}

func (s *fuseTestSuite) TestMultipleOpens() {
	datasetName := "TestMultipleOpens"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	file1, code := testfs.Create("contend", uint32(os.O_CREATE|os.O_RDWR), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)

	file2, code := testfs.Open("contend", uint32(os.O_RDWR), nil)
	assert.Equal(s.T(), fuse.OK, code)

	file1.Write([]byte("abc contact"), 0)
	file2.Write([]byte("321"), 0)

	data := make([]byte, 11)
	rr, code := file1.Read(data, 0)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), 11, rr.Size())
	assert.Equal(s.T(), "321 contact", string(data))
}
