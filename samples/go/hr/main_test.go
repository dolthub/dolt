// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"path"
	"runtime"
	"testing"

	"github.com/attic-labs/noms/samples/go/test_util"
	"github.com/attic-labs/testify/suite"
)

func TestBasics(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	test_util.ClientTestSuite
}

func (s *testSuite) TestRoundTrip() {
	spec := fmt.Sprintf("ldb:%s::hr", s.LdbDir)
	out := s.Run(main, []string{"-ds", spec, "list-persons"})
	s.Equal("No people found\n", out)

	out = s.Run(main, []string{"-ds", spec, "add-person", "42", "Benjamin Kalman", "Programmer, Barista"})
	s.Equal("", out)

	out = s.Run(main, []string{"-ds", spec, "add-person", "43", "Abigail Boodman", "Chief Architect"})
	s.Equal("", out)

	out = s.Run(main, []string{"-ds", spec, "list-persons"})
	s.Equal(`Benjamin Kalman (id: 42, title: Programmer, Barista)
Abigail Boodman (id: 43, title: Chief Architect)
`, out)
}

func (s *testSuite) ReadCanned() {
	_, p, _, _ := runtime.Caller(0)
	p = path.Join(path.Dir(p), "test-data")
	out := s.Run(main, []string{"-ds", fmt.Sprintf("ldb:%s::hr", p), "list-persons"})
	s.Equal(`Aaron Boodman (id: 7, title: Chief Evangelism Officer)
Samuel Boodman (id: 13, title: VP, Culture)
`, out)
}
