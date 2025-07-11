package mcp

import (
	"database/sql"
	"fmt"
	"testing"

	"context"

	"os"
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
	t                     *testing.T
	doltDatabaseParentDir string
	doltDatabaseDir       string
	dsn                   string
	testDb                *sql.DB
	doltServer            benchmark_runner.Server
}

func (s *testSuite) Setup(t *testing.T) {
	s.t = t
	// todo: checkout a new testing branch
	// todo: optionally allow tests to modify database before test run
}

func (s *testSuite) Teardown(t *testing.T) {
	s.t = t
	// todo: checkout the main branch, then delete the testing branch
}

func createMCPDoltServerTestSuite(ctx context.Context, doltBinPath string) (*testSuite, error) {
	doltDatabaseParentDir, err := os.MkdirTemp("", "mcp-server-tests-*")
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

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true&parseTime=true", mcpTestUserName, mcpTestPassword, doltServerHost, doltServerPort, mcpTestDatabaseName)
	testDb, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = testDb.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &testSuite{
		dsn:                   dsn,
		doltServer:            doltServer,
		doltDatabaseParentDir: doltDatabaseParentDir,
		doltDatabaseDir:       doltDatabaseDir,
		testDb:                testDb,
	}, nil
}

func teardownMCPDoltServerTestSuite(s *testSuite) {
	if s == nil {
		return
	}

	defer func() {
		os.RemoveAll(s.doltDatabaseParentDir)
	}()

	if s.testDb != nil {
		s.testDb.Close()
		s.testDb = nil
	}

	if s.doltServer != nil {
		s.doltServer.Stop()
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
