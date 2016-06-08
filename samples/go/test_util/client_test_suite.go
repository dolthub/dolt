// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package test_util

import (
	"flag"
	"io/ioutil"
	"os"
	"path"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/testify/suite"
)

type ClientTestSuite struct {
	suite.Suite
	TempDir string
	LdbDir  string
	out     *os.File
}

func (suite *ClientTestSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "nomstest")
	d.Chk.NoError(err)
	out, err := ioutil.TempFile(dir, "out")
	d.Chk.NoError(err)

	suite.TempDir = dir
	suite.LdbDir = path.Join(dir, "ldb")
	suite.out = out
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.out.Close()
	defer d.Chk.NoError(os.RemoveAll(suite.TempDir))
}

func (suite *ClientTestSuite) Run(m func(), args []string) string {
	origArgs := os.Args
	origOut := os.Stdout
	origErr := os.Stderr

	os.Args = append([]string{"cmd"}, args...)
	os.Stdout = suite.out

	defer func() {
		os.Args = origArgs
		os.Stdout = origOut
		os.Stderr = origErr
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	m()

	_, err := suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	b, err := ioutil.ReadAll(os.Stdout)
	d.Chk.NoError(err)

	_, err = suite.out.Seek(0, 0)
	d.Chk.NoError(err)
	err = suite.out.Truncate(0)
	d.Chk.NoError(err)

	return string(b)
}
