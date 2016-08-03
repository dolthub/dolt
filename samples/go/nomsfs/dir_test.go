// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"sort"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func TestDir(t *testing.T) {
	suite.Run(t, &fuseTestSuite{})
}

func findDir(fs pathfs.FileSystem, path string, delimiter string) []string {
	ents, _ := fs.OpenDir(path, nil)
	ret := make([]string, 0)
	for _, ent := range ents {
		child := path + delimiter + ent.Name
		attr, _ := fs.GetAttr(child, nil)
		if attr.Mode&fuse.S_IFDIR != 0 {
			ret = append(ret, child+"/")
			ret = append(ret, findDir(fs, child, "/")...)
		} else {
			ret = append(ret, child)
		}
	}

	return ret
}

func find(fs pathfs.FileSystem) []string {
	return findDir(fs, "", "")
}

func (s *fuseTestSuite) TestHierarchy() {
	datasetName := "TestHierarchy"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	hierarchy := []string{
		"bin/",
		"bin/sh",
		"usr/",
		"usr/bin/",
		"usr/bin/cat",
		"usr/bin/bash",
		"usr/lib/",
		"usr/lib/libc.so.1",
		"usr/dict/",
		"usr/dict/words",
		"usr/dict/words2",
	}

	for _, path := range hierarchy {
		if ll := len(path); path[ll-1] == '/' {
			code := testfs.Mkdir(path[:ll-1], 0555, nil)
			assert.Equal(s.T(), fuse.OK, code)
		} else {
			_, code := testfs.Create(path, uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0444, nil)
			assert.Equal(s.T(), fuse.OK, code)
		}
	}

	h := find(testfs)

	sort.Strings(hierarchy)
	sort.Strings(h)

	assert.Equal(s.T(), hierarchy, h)
}

func (s *fuseTestSuite) TestDirError() {
	datasetName := "TestDirError"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("foo/bar", 0755, nil)
	assert.Equal(s.T(), fuse.ENOENT, code)
	_, code = testfs.Create("foo", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("foo/bar", 0755, nil)
	assert.Equal(s.T(), fuse.ENOTDIR, code)

	_, code = testfs.OpenDir("foo", nil)
	assert.Equal(s.T(), fuse.ENOTDIR, code)
}

func (s *fuseTestSuite) TestRenaming() {
	datasetName := "TestRenaming"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("foo", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("foo/bar", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("foo/baz", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	_, code = testfs.Create("foo/bar/buzz", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)

	code = testfs.Rename("foo/bar/buzz", "foo/baz/buzz", nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Rename("foo/baz/buzz", "buzz", nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Rename("buzz", "foo/buzz", nil)
	assert.Equal(s.T(), fuse.OK, code)
}

func (s *fuseTestSuite) TestRenameWhileOpen() {
	datasetName := "TestRenameWhileOpen"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	var testfs pathfs.FileSystem

	start(str, func(fs pathfs.FileSystem) { testfs = fs })

	code := testfs.Mkdir("foo", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	code = testfs.Mkdir("foo/bar", 0755, nil)
	assert.Equal(s.T(), fuse.OK, code)
	file, code := testfs.Create("foo/bar/file.txt", uint32(os.O_CREATE)|uint32(os.O_WRONLY), 0644, nil)
	assert.Equal(s.T(), fuse.OK, code)

	// Validate renaming a file between opening it and writing to it.
	code = testfs.Rename("foo/bar/file.txt", "file.txt", nil)
	assert.Equal(s.T(), fuse.OK, code)

	n, code := file.Write([]byte("howdy!"), 0)
	assert.Equal(s.T(), uint32(6), n)
	assert.Equal(s.T(), fuse.OK, code)
	assertAttr(s, testfs, "file.txt", 0644|fuse.S_IFREG, 6)
}
