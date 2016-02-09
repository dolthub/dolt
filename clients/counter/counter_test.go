package main

import (
	"testing"

	"github.com/attic-labs/noms/clients/util"
	"github.com/stretchr/testify/suite"
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
