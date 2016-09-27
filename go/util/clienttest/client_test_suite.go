// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package clienttest

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/testify/suite"
	flag "github.com/juju/gnuflag"
)

type ClientTestSuite struct {
	suite.Suite
	TempDir    string
	LdbDir     string
	ExitStatus int
	out        *os.File
	err        *os.File
}

type ExitError struct {
	Code int
}

func (suite *ClientTestSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "nomstest")
	d.Chk.NoError(err)
	stdOutput, err := ioutil.TempFile(dir, "out")
	d.Chk.NoError(err)
	errOutput, err := ioutil.TempFile(dir, "err")
	d.Chk.NoError(err)

	suite.TempDir = dir
	suite.LdbDir = path.Join(dir, "ldb")
	suite.out = stdOutput
	suite.err = errOutput
	exit.Exit = mockExit
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.out.Close()
	suite.err.Close()
	defer d.Chk.NoError(os.RemoveAll(suite.TempDir))
}

// MustRun is a wrapper around Run that will panic on Exit or Panic
func (suite *ClientTestSuite) MustRun(m func(), args []string) (stdout string, stderr string) {
	var err interface{}
	if stdout, stderr, err = suite.Run(m, args); err != nil {
		panic(err)
	}
	return
}

// Run will execute a function passing to it commandline args, and captures stdout,stderr.
// If m()  panics the panic is caught, and returned with recoveredError
// If m() calls exit.Exit() m() will panic and return ExitError with recoveredError
func (suite *ClientTestSuite) Run(m func(), args []string) (stdout string, stderr string, recoveredErr interface{}) {
	origArgs := os.Args
	origOut := os.Stdout
	origErr := os.Stderr

	os.Args = append([]string{"cmd"}, args...)
	os.Stdout = suite.out
	os.Stderr = suite.err

	defer func() {
		recoveredErr = recover()
		_, err := suite.out.Seek(0, 0)
		d.Chk.NoError(err)
		capturedOut, err := ioutil.ReadAll(os.Stdout)
		d.Chk.NoError(err)

		_, err = suite.out.Seek(0, 0)
		d.Chk.NoError(err)
		err = suite.out.Truncate(0)
		d.Chk.NoError(err)

		_, err = suite.err.Seek(0, 0)
		d.Chk.NoError(err)
		capturedErr, err := ioutil.ReadAll(os.Stderr)
		d.Chk.NoError(err)

		_, err = suite.err.Seek(0, 0)
		d.Chk.NoError(err)
		err = suite.err.Truncate(0)
		d.Chk.NoError(err)
		os.Args = origArgs
		os.Stdout = origOut
		os.Stderr = origErr
		stdout, stderr = string(capturedOut), string(capturedErr)
	}()

	suite.ExitStatus = 0
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	m()
	return
}

// Mock exit.Exit() implementation for use during testing.
func mockExit(status int) {
	panic(ExitError{status})
}
