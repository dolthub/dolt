// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package runner

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/attic-labs/testify/suite"
)

const (
	boilerplate = `
from __future__ import print_function
import os, sys

%s
`
	buildFileBasename = "build.py"
)

func TestSerialRunnerTestSuite(t *testing.T) {
	suite.Run(t, &SerialRunnerTestSuite{})
}

type SerialRunnerTestSuite struct {
	suite.Suite
	dir   string
	index int
}

func (suite *SerialRunnerTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
}

func (suite *SerialRunnerTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}

func (suite *SerialRunnerTestSuite) TestForceRunInDir() {
	scriptPath := filepath.Join(suite.dir, buildFileBasename)
	suite.makeTestBuildFile(scriptPath, []string{"print(os.getcwd(), file=sys.stdout)"})

	old := os.Stdout // keep backup of the real stdout
	r, w, err := os.Pipe()
	suite.NoError(err)
	os.Stdout = w
	defer func() { os.Stdout = old }()
	defer r.Close()

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		buf := &bytes.Buffer{}
		io.Copy(buf, r)
		outC <- buf.String()
	}()

	ForceRunInDir(suite.dir, nil, "python", scriptPath)

	w.Close()
	out := strings.TrimSpace(<-outC)
	actualSuiteDir, err := filepath.EvalSymlinks(suite.dir)
	suite.NoError(err)
	suite.Equal(actualSuiteDir, out)
}

func (suite *SerialRunnerTestSuite) TestRunInDir() {
	scriptPath := filepath.Join(suite.dir, buildFileBasename)
	suite.makeTestBuildFile(scriptPath, []string{
		"print(os.getcwd(), file=sys.stdout)",
		"print('error', file=sys.stderr)",
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	RunInDir(stdout, stderr, suite.dir, "python", scriptPath)
	actualSuiteDir, err := filepath.EvalSymlinks(suite.dir)
	suite.NoError(err)
	suite.Equal(actualSuiteDir, strings.TrimSpace(string(stdout.Bytes())))
	suite.Equal("error", strings.TrimSpace(string(stderr.Bytes())))
}

func (suite *SerialRunnerTestSuite) TestEnvVars() {
	makeEnvVarPrintBuildFile := func(path, varname string) {
		fmtStatement := fmt.Sprintf(`print(os.environ['%s'], file=sys.stdout)`, varname)
		suite.makeTestBuildFile(path, []string{fmtStatement})
	}

	type testCase struct {
		path, varname, expected string
	}
	env := Env{
		"PATH":                os.Getenv("PATH"),
		"GOPATH":              os.Getenv("GOPATH"),
		"NOMS_CHECKOUT_PATH":  "/where/noms/is",
		"ATTIC_CHECKOUT_PATH": "/where/attic/is",
	}
	tests := []testCase{}
	for n, v := range env {
		tc := testCase{suite.uniqueBuildFile(), n, v}
		makeEnvVarPrintBuildFile(tc.path, tc.varname)
		tests = append(tests, tc)
	}
	gorootTestCase := testCase{suite.uniqueBuildFile(), "GOROOT", runtime.GOROOT()}
	makeEnvVarPrintBuildFile(gorootTestCase.path, gorootTestCase.varname)
	tests = append(tests, gorootTestCase)

	log := &bytes.Buffer{}
	if suite.True(Serial(log, log, env, suite.dir, buildFileBasename), "Serial() should have succeeded! logs:\n%s", string(log.Bytes())) {
		logText := string(log.Bytes())
		for _, tc := range tests {
			suite.Contains(logText, tc.expected)
		}
	}
}

func (suite *SerialRunnerTestSuite) TestFailure() {
	type testCase struct {
		path, expected string
	}
	tests := []testCase{
		{suite.uniqueBuildFile(), "Scoobaz"},
		{suite.uniqueBuildFile(), "at the disco"},
	}
	goodOne := testCase{suite.uniqueBuildFile(), "All's well"}

	suite.makeTestBuildFile(tests[0].path, []string{"Scoobaz() # Won't compile."})
	suite.makeTestBuildFile(tests[1].path, []string{`assert(False, "at the disco") # Won't run.`})
	suite.makeTestBuildFile(goodOne.path, []string{fmt.Sprintf(`print "%s"`, goodOne.expected)})

	log := &bytes.Buffer{}
	suite.False(Serial(log, log, Env{}, suite.dir, buildFileBasename))
	logText := string(log.Bytes())
	suite.Contains(logText, tests[0].expected)
	suite.Contains(logText, tests[1].expected)
}

func (suite *SerialRunnerTestSuite) uniqueBuildFile() string {
	suite.index++
	return filepath.Join(suite.dir, fmt.Sprintf("%d", suite.index), buildFileBasename)
}

func (suite *SerialRunnerTestSuite) makeTestBuildFile(path string, statements []string) {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, boilerplate, strings.Join(statements, "\n"))
	err := os.MkdirAll(filepath.Dir(path), 0777)
	suite.NoError(err)
	err = ioutil.WriteFile(path, buf.Bytes(), 0755)
	suite.NoError(err)
}
