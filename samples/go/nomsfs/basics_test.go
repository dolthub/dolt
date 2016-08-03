// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type fuseTestSuite struct {
	clienttest.ClientTestSuite
}

func assertAttr(s *fuseTestSuite, fs pathfs.FileSystem, path string, mode uint32, size uint64) {
	attr, code := fs.GetAttr(path, nil)
	assert.Equal(s.T(), fuse.OK, code)
	if code == fuse.OK {
		assert.Equal(s.T(), mode, attr.Mode)
		assert.Equal(s.T(), size, attr.Size)
	}
}

func TestBasics(t *testing.T) {
	suite.Run(t, &fuseTestSuite{})
}

func (s *fuseTestSuite) TestDir() {
	datasetName := "TestDir"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("noms", 0777, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "noms", 0777|fuse.S_IFDIR, 0)
}

func (s *fuseTestSuite) TestDirInDir() {
	datasetName := "TestDirInDir"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("opt", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("opt/SUNWdtrt", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "opt", 0755|fuse.S_IFDIR, 1)
	assertAttr(s, testfs, "opt/SUNWdtrt", 0755|fuse.S_IFDIR, 0)
}

func (s *fuseTestSuite) TestFile() {
	datasetName := "TestFile"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	_, code := testfs.Create("pokemon.go", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "pokemon.go", 0644|fuse.S_IFREG, 0)
}

func (s *fuseTestSuite) TestFileInDir() {
	datasetName := "TestFile"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("usr", 0555, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("usr/sbin", 0555, nil)
	assert.Equal(s.T(), fuse.OK, code)
	_, code = testfs.Create("usr/sbin/dtrace", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0555, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "usr", 0555|fuse.S_IFDIR, 1)
	assertAttr(s, testfs, "usr/sbin", 0555|fuse.S_IFDIR, 1)
	assertAttr(s, testfs, "usr/sbin/dtrace", 0555|fuse.S_IFREG, 0)
}

func (s *fuseTestSuite) TestSymlink() {
	datasetName := "TestSymlink"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Symlink("there", "here", nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "here", 0755|fuse.S_IFLNK, 0)
	value, code := testfs.Readlink("here", nil)
	assert.Equal(s.T(), fuse.OK, code)
	assert.Equal(s.T(), "there", value)
}

func (s *fuseTestSuite) TestChmod() {
	datasetName := "TestChmod"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	_, code := testfs.Create("passwords.txt", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0777, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Chmod("passwords.txt", 0444, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "passwords.txt", 0444|fuse.S_IFREG, 0)
}

func (s *fuseTestSuite) TestRename() {
	datasetName := "TestRename"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	_, code := testfs.Create("prince", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Rename("prince", "O(+>", nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "O(+>", 0644|fuse.S_IFREG, 0)
}

func (s *fuseTestSuite) TestUnlink() {
	datasetName := "TestUnlink"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	_, code := testfs.Create("dilma", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "", 0777|fuse.S_IFDIR, 1) // 1 means it's there
	code = testfs.Unlink("dilma", nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "", 0777|fuse.S_IFDIR, 0) // 0 means no entries
}

func (s *fuseTestSuite) TestRmdir() {
	datasetName := "TestRmdir"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("wikileaks", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "", 0777|fuse.S_IFDIR, 1) // 1 means it's there
	code = testfs.Rmdir("wikileaks", nil)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "", 0777|fuse.S_IFDIR, 0) // 0 means no entries
}
