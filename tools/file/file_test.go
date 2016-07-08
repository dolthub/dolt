// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/testify/suite"
)

const (
	contents = "hey"
)

func TestSerialRunnerTestSuite(t *testing.T) {
	suite.Run(t, &FileTestSuite{})
}

type FileTestSuite struct {
	suite.Suite
	dir, src, exc string
}

func (suite *FileTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.src = filepath.Join(suite.dir, "srcfile")
	suite.exc = filepath.Join(suite.dir, "excfile")
	suite.NoError(ioutil.WriteFile(suite.src, []byte(contents), 0644))
	suite.NoError(ioutil.WriteFile(suite.exc, []byte(contents), 0755))
}

func (suite *FileTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}

func (suite *FileTestSuite) TestCopyFile() {
	dst := filepath.Join(suite.dir, "dstfile")

	test := func(src string, mode int) {
		DumbCopy(src, dst)

		info, err := os.Stat(src)
		suite.NoError(err)
		suite.Equal(mode, int(info.Mode()))

		out, err := ioutil.ReadFile(dst)
		suite.NoError(err)
		suite.Equal(contents, string(out))
	}

	test(suite.src, 0644)
	test(suite.exc, 0755)
}

func (suite *FileTestSuite) TestCopyLink() {
	link := filepath.Join(suite.dir, "link")
	suite.NoError(os.Symlink(suite.src, link))

	dst := filepath.Join(suite.dir, "dstfile")
	DumbCopy(link, dst)

	info, err := os.Lstat(dst)
	suite.NoError(err)
	suite.True(info.Mode().IsRegular())

	out, err := ioutil.ReadFile(dst)
	suite.NoError(err)
	suite.Equal(contents, string(out))
}

func (suite *FileTestSuite) TestNoCopyDir() {
	dir, err := ioutil.TempDir(suite.dir, "")
	suite.NoError(err)

	dst := filepath.Join(suite.dir, "dst")
	suite.Error(d.Try(func() { DumbCopy(dir, dst) }))
}
