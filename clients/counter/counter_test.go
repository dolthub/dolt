package main

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/clients/util"
)

func TestCounter(t *testing.T) {
	suite.Run(t, &counterTestSuite{})
}

type counterTestSuite struct {
	util.ClientTestSuite
}

func (s *counterTestSuite) TestCounter() {
	args := []string{"-ds", "counter"}
	s.Equal("1\n", s.Run(main, args))
	s.Equal("2\n", s.Run(main, args))
	s.Equal("3\n", s.Run(main, args))
}
