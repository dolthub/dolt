// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

// IntegrationSuiteInterface is the interface that IntegrationSuite implements.
type IntegrationSuiteInterface interface {
	suite.TestingSuite
	// DatabaseSpecString returns the spec for the test database.
	DatabaseSpecString() string
	// ValueSpecString returns the spec for the value in the test database.
	ValueSpecString(value string) string
	// Database is the underlying db that is bing changed.
	Database() datas.Database
	// NodeOutput is the result (stdout) of running the node program.
	NodeOutput() string

	setPort(port int)
	setCs(cs chunks.ChunkStore)
	setNodeOut(out string)
	npmInstall()
}

// SetupSuite is the interface to implement if you need to run some code before the server is started.
type SetupSuite interface {
	Setup()
}

// TeardownSuite is the interface to implement if you want to run some code after the server is stopped.
type TeardownSuite interface {
	Teardown()
}

// NodeArgsSuite is the interface to implement if you want to provide extra arguments to node. If this is not implemented we call `node .`
type NodeArgsSuite interface {
	NodeArgs() []string
}

// IntegrationSuite is used to create a single node js integration test.
type IntegrationSuite struct {
	suite.Suite
	cs   chunks.ChunkStore
	port int
	out  string
}

func Run(t *testing.T, s IntegrationSuiteInterface) {
	s.SetT(t)
	s.npmInstall()
	cs := chunks.NewMemoryStore()
	s.setCs(cs)

	if s, ok := s.(SetupSuite); ok {
		s.Setup()
	}

	runServer(s, cs)

	if s, ok := s.(TeardownSuite); ok {
		s.Teardown()
	}
}

func runServer(s IntegrationSuiteInterface, cs chunks.ChunkStore) {
	server := datas.NewRemoteDatabaseServer(cs, 0)
	server.Ready = func() {
		s.setPort(server.Port())
		runNode(s)
		server.Stop()
	}
	server.Run()
}

func runNode(s IntegrationSuiteInterface) {
	args := []string{"."}
	if ns, ok := s.(NodeArgsSuite); ok {
		args = append(args, ns.NodeArgs()...)
	}
	cmd := exec.Command("node", args...)
	cmd.Stderr = os.Stderr
	var buf bytes.Buffer
	cmd.Stdout = &buf
	err := cmd.Run()
	assert.NoError(s.T(), err)
	s.setNodeOut(buf.String())
}

func (s *IntegrationSuite) setPort(port int) {
	s.port = port
}

func (s *IntegrationSuite) setCs(cs chunks.ChunkStore) {
	s.cs = cs
}

func (s *IntegrationSuite) Database() datas.Database {
	return datas.NewDatabase(s.cs)
}

func (s *IntegrationSuite) setNodeOut(out string) {
	s.out = out
}

func (s *IntegrationSuite) NodeOutput() string {
	return s.out
}

func (s *IntegrationSuite) npmInstall() {
	err := exec.Command("npm", "install").Run()
	s.NoError(err)
}

// DatabaseSpecString returns the spec for the database to test against.
func (s *IntegrationSuite) DatabaseSpecString() string {
	return spec.CreateDatabaseSpecString("http", fmt.Sprintf("//localhost:%d", s.port))
}

// ValueSpecString returns the value spec for the value to test against.
func (s *IntegrationSuite) ValueSpecString(value string) string {
	return spec.CreateValueSpecString("http", fmt.Sprintf("//localhost:%d", s.port), value)
}
