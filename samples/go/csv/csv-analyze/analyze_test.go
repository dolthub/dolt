// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestCSVAnalyze(t *testing.T) {
	suite.Run(t, &csvAnalyzeTestSuite{})
}

type csvAnalyzeTestSuite struct {
	clienttest.ClientTestSuite
	tmpFileName string
}

func writeSampleData(w io.Writer) {
	_, err := io.WriteString(w, "Date,Time,Temperature\n")
	d.Chk.NoError(err)

	// 30 samples of String,String,Number
	for i := 0; i < 30; i++ {
		_, err = io.WriteString(w, fmt.Sprintf("08/14/2016,12:%d,73.4%d\n", i, i))
		d.Chk.NoError(err)
	}

	// an extra sample of String,String,String to have detect types with smaller samples only find Number
	_, err = io.WriteString(w, fmt.Sprintf("08/14/2016,13:01,none\n"))
	d.Chk.NoError(err)

	// an extra sample of a duplicate Date,Temperature to have detect pk rule it out (with smaller samples)
	_, err = io.WriteString(w, fmt.Sprintf("08/14/2016,13:02,73.42\n"))
	d.Chk.NoError(err)
}

func (s *csvAnalyzeTestSuite) SetupTest() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	s.tmpFileName = input.Name()
	writeSampleData(input)
	defer input.Close()
}

func (s *csvAnalyzeTestSuite) TearDownTest() {
	os.Remove(s.tmpFileName)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectColumnTypes() {
	stdout, stderr := s.MustRun(main, []string{"--detect-column-types=1", s.tmpFileName})
	s.Equal("String,String,String\n", stdout)
	s.Equal("", stderr)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectColumnTypesSamples20() {
	stdout, stderr := s.MustRun(main, []string{"--detect-column-types=1", "--num-samples=20", s.tmpFileName})
	s.Equal("String,String,Number\n", stdout)
	s.Equal("", stderr)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectPrimaryKeys() {
	stdout, stderr := s.MustRun(main, []string{"--detect-pk=1", s.tmpFileName})
	s.Equal("Time\nDate,Time\nTime,Temperature\nDate,Time,Temperature\n", stdout)
	s.Equal("", stderr)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectPrimaryKeysSamples20() {
	stdout, stderr := s.MustRun(main, []string{"--detect-pk=1", "--num-samples=20", s.tmpFileName})
	s.Equal("Time\nTemperature\nDate,Time\nDate,Temperature\nTime,Temperature\nDate,Time,Temperature\n", stdout)
	s.Equal("", stderr)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectPrimaryKeysSingleField() {
	stdout, stderr := s.MustRun(main, []string{"--detect-pk=1", "--num-fields-pk=1", s.tmpFileName})
	s.Equal("Time\n", stdout)
	s.Equal("", stderr)
}

func (s *csvAnalyzeTestSuite) TestCSVAnalyzeDetectPrimaryKeysTwoFields() {
	stdout, stderr := s.MustRun(main, []string{"--detect-pk=1", "--num-fields-pk=2", s.tmpFileName})
	s.Equal("Time\nDate,Time\nTime,Temperature\n", stdout)
	s.Equal("", stderr)
}
