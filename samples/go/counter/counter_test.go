// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestCounter(t *testing.T) {
	suite.Run(t, &counterTestSuite{})
}

type counterTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *counterTestSuite) TestCounter() {
	spec := spec.CreateValueSpecString("ldb", s.LdbDir, "counter")
	args := []string{spec}
	stdout, stderr := s.MustRun(main, args)
	s.Equal("1\n", stdout)
	s.Equal("", stderr)
	stdout, stderr = s.MustRun(main, args)
	s.Equal("2\n", stdout)
	s.Equal("", stderr)
	stdout, stderr = s.MustRun(main, args)
	s.Equal("3\n", stdout)
	s.Equal("", stderr)
}
