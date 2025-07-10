package mcp

import (
	"testing"

	"context"

	"syscall"
	"os/exec"
	"github.com/dolthub/dolt/go/performance/utils/benchmark_runner"
)

const (
	mcpTestDatabaseName = "root"
	mcpTestUserName = "mcp"
	mcpTestPassword = "passw0rd"
	doltServerHost = "0.0.0.0"
	doltServerPort = 3306
)

type testSuite struct {
	t *testing.T
	doltServer benchmark_runner.Server
}

func (s *testSuite) Setup() {
	ctx := context.Background()
	
	doltDatabaseDir := s.t.TempDir()
		
	doltBinPath, err := exec.LookPath("dolt")
	if err != nil {
		s.t.Skip("dolt binary not found in PATH, skipping mcp test")
	}

	serverArgs := []string{
		"-l",
		"debug",
	}

	doltServerConfig := benchmark_runner.NewDoltServerConfig(
		"", 
		doltBinPath, 
		mcpTestUserName, 
		doltServerHost,
		"",
		"",
		benchmark_runner.CpuServerProfile,
		doltServerPort,
		serverArgs,
	)

	serverParams, err := doltServerConfig.GetServerArgs()
	if err != nil {
		s.t.Fatalf("failed to get Dolt server args from config: %s", err.Error())
	}

	doltServer := benchmark_runner.NewServer(ctx, doltDatabaseDir, doltServerConfig, syscall.SIGTERM, serverParams)
	err = doltServer.Start()
	if err != nil {
		s.t.Fatalf("failed to start Dolt server: %s", err.Error())
	}

	s.doltServer = doltServer
}

func (s *testSuite) Teardown() {
	if s.doltServer != nil {
		err := s.doltServer.Stop()
		if err != nil {
			s.t.Fatalf("failed to terminate container: %s", err)
		}
		s.doltServer = nil
	}
}

func RunTest(t *testing.T, testName string, testFunc func(s *testSuite)) {
	t.Run(testName, func(t *testing.T) {
		suite := &testSuite{t: t}
		suite.Setup()
		defer suite.Teardown()
		testFunc(suite)
	})
}

