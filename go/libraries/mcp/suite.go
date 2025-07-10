package mcp

import (
	"database/sql"
	"fmt"
	"testing"

	"context"

	"os"
	"os/exec"
	"syscall"

	"github.com/dolthub/dolt/go/performance/utils/benchmark_runner"
	"github.com/dolthub/dolt/go/store/constants"
)

const (
	mcpTestDatabaseName = "test"
	mcpTestUserName     = "root"
	mcpTestPassword     = ""
	doltServerHost      = "0.0.0.0"
	doltServerPort      = 3306
)

type testSuite struct {
	t          *testing.T
	doltDatabaseParentDir string
	doltDatabaseDir string
	dsn string
	testDb *sql.DB
	doltServer benchmark_runner.Server
}

var suite *testSuite

func TestMain(m *testing.M) {
	ctx := context.Background()

	doltBinPath, err := exec.LookPath("dolt")
	if err != nil {
		fmt.Println("dolt binary not found in PATH, skipping mcp test")
		os.Exit(0)
	}
	
	suite, err = createMCPDoltServerTestSuite(ctx, doltBinPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create dolt server test suite: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		teardownMCPDoltServerTestSuite(suite)
		os.RemoveAll(suite.doltDatabaseParentDir)
	}()

	code := m.Run()

	os.Exit(code)
}

func createMCPDoltServerTestSuite(ctx context.Context, doltBinPath string) (*testSuite, error) {
	doltDatabaseParentDir, err := os.MkdirTemp("", "mcp-tests-*")
	if err != nil {
		return nil, err
	}

	doltDatabaseDir, err := benchmark_runner.InitDoltRepo(ctx, doltDatabaseParentDir, doltBinPath, constants.FormatDefaultString, mcpTestDatabaseName)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	doltServer := benchmark_runner.NewServer(ctx, doltDatabaseDir, doltServerConfig, syscall.SIGTERM, serverParams)
	err = doltServer.Start()
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mcpTestUserName, mcpTestPassword, doltServerHost, doltServerPort, mcpTestDatabaseName)
	testDb, err := sql.Open("mysql", dsn)
    if err != nil {
		return nil, err
    }

	err = testDb.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &testSuite{
		dsn: dsn,
		doltServer: doltServer,
		doltDatabaseParentDir: doltDatabaseParentDir,
		doltDatabaseDir: doltDatabaseDir,
		testDb: testDb,
	}, nil
}

func (s *testSuite) Setup(t *testing.T) {
	s.t = t
}

func (s *testSuite) Teardown(t *testing.T) {
	s.t = t
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
		suite.Setup(t)
		defer suite.Teardown(t)
		testFunc(suite)
	})
}

