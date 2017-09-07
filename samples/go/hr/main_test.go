// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"path"
	"runtime"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/stretchr/testify/suite"
)

func TestBasics(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func (s *testSuite) TestRoundTrip() {
	spec := spec.CreateValueSpecString("nbs", s.DBDir, "hr")
	stdout, stderr := s.MustRun(main, []string{"--ds", spec, "list-persons"})
	s.Equal("No people found\n", stdout)
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"--ds", spec, "add-person", "42", "Benjamin Kalman", "Programmer, Barista"})
	s.Equal("", stdout)
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"--ds", spec, "add-person", "43", "Abigail Boodman", "Chief Architect"})
	s.Equal("", stdout)
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"--ds", spec, "list-persons"})
	s.Equal(`Benjamin Kalman (id: 42, title: Programmer, Barista)
Abigail Boodman (id: 43, title: Chief Architect)
`, stdout)
	s.Equal("", stderr)

}

func (s *testSuite) TestReadCanned() {
	_, p, _, _ := runtime.Caller(0)
	p = path.Join(path.Dir(p), "test-data")

	stdout, stderr := s.MustRun(main, []string{"--ds", spec.CreateValueSpecString("nbs", p, "hr"), "list-persons"})
	s.Equal(`Aaron Boodman (id: 7, title: Chief Evangelism Officer)
Samuel Boodman (id: 13, title: VP, Culture)
`, stdout)
	s.Equal("", stderr)
}

func (s *testSuite) TestInvalidDatasetSpec() {
	// Should not crash
	_, _ = s.MustRun(main, []string{"--ds", "invalid-dataset", "list-persons"})
}
