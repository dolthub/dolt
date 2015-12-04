package runner

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/tools/imports"
)

const (
	boilerplate = `
package main

func main() {
	%s
}
`
	buildFileBasename = "build.go"
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

func (suite *SerialRunnerTestSuite) TestEnvVars() {
	makeEnvVarPrintBuildFile := func(path, varname string) {
		// This creates a string containing a Go statement that uses fmt.Println to print the value of the given environment variable. Looking at it is super confusing to me; hence the comment.
		fmtStatement := fmt.Sprintf(`fmt.Println(os.Getenv("%s"))`, varname)
		suite.makeTestBuildFile(path, []string{fmtStatement})
	}

	type testCase struct {
		path, varname, expected string
	}
	env := Env{
		"PATH":                os.Getenv("PATH"),
		"NOMS_CHECKOUT_PATH":  "/where/noms/is",
		"ATTIC_CHECKOUT_PATH": "/where/attic/is",
	}
	tests := []testCase{}
	for n, v := range env {
		tc := testCase{suite.uniqueBuildFile(), n, v}
		makeEnvVarPrintBuildFile(tc.path, tc.varname)
		tests = append(tests, tc)
	}
	log := &bytes.Buffer{}
	suite.True(Serial(log, log, env, suite.dir, buildFileBasename), "Serial() should have succeeded! logs:\n%s", string(log.Bytes()))
	logText := string(log.Bytes())
	for _, tc := range tests {
		suite.Contains(logText, tc.expected)
	}
}

func (suite *SerialRunnerTestSuite) TestFailure() {
	type testCase struct {
		path, expected string
	}
	tests := []testCase{
		testCase{suite.uniqueBuildFile(), "Scoobaz"},
		testCase{suite.uniqueBuildFile(), "at the disco"},
	}
	goodOne := testCase{suite.uniqueBuildFile(), "All's well"}

	suite.makeTestBuildFile(tests[0].path, []string{"Scoobaz() // Won't compile."})
	suite.makeTestBuildFile(tests[1].path, []string{`panic("at the disco") // Won't run.`})
	suite.makeTestBuildFile(goodOne.path, []string{fmt.Sprintf(`fmt.Println("%s")`, goodOne.expected)})

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
	bs, err := imports.Process("", buf.Bytes(), nil)
	suite.NoError(err)
	err = os.MkdirAll(filepath.Dir(path), 0777)
	suite.NoError(err)
	err = ioutil.WriteFile(path, bs, 0755)
	suite.NoError(err)
}
