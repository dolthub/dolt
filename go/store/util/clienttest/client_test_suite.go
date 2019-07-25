// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package clienttest

import (
	"io/ioutil"
	"os"
	"path"

	flag "github.com/juju/gnuflag"
	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/util/exit"
)

const DefaultMemTableSize = 8 * (1 << 20) // 8MB

type ClientTestSuite struct {
	suite.Suite
	TempDir    string
	DBDir      string
	DBDir2     string
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
	suite.DBDir = path.Join(dir, "db")
	suite.DBDir2 = path.Join(suite.TempDir, "db2")
	suite.out = stdOutput
	suite.err = errOutput
	exit.Exit = MockExit

	os.Mkdir(suite.DBDir, 0777)
	os.Mkdir(suite.DBDir2, 0777)
}

func (suite *ClientTestSuite) TearDownSuite() {
	suite.out.Close()
	suite.err.Close()
	err := os.RemoveAll(suite.TempDir)
	if !osutil.IsWindows {
		d.Chk.NoError(err)
	}
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

		// Reset everything right away so that error-checking below goes to terminal.
		os.Args = origArgs
		os.Stdout = origOut
		os.Stderr = origErr

		_, err := suite.out.Seek(0, 0)
		d.Chk.NoError(err)
		capturedOut, err := ioutil.ReadAll(suite.out)
		d.Chk.NoError(err)

		_, err = suite.out.Seek(0, 0)
		d.Chk.NoError(err)
		err = suite.out.Truncate(0)
		d.Chk.NoError(err)

		_, err = suite.err.Seek(0, 0)
		d.Chk.NoError(err)
		capturedErr, err := ioutil.ReadAll(suite.err)
		d.Chk.NoError(err)

		_, err = suite.err.Seek(0, 0)
		d.Chk.NoError(err)
		err = suite.err.Truncate(0)
		d.Chk.NoError(err)
		stdout, stderr = string(capturedOut), string(capturedErr)
	}()

	suite.ExitStatus = 0
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	m()
	return
}

// Mock exit.Exit() implementation for use during testing.
func MockExit(status int) {
	panic(ExitError{status})
}
