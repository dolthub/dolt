package mcp

import (
	"database/sql"
	"fmt"
	"testing"

	"context"

	"errors"
	"os"
	"syscall"

	"github.com/dolthub/dolt/go/performance/utils/benchmark_runner"
	"github.com/dolthub/dolt/go/store/constants"
)

const (
	mcpTestDatabaseName   = "test"
	mcpTestRootUserName   = "root"
	mcpTestClientUserName = "mcp"
	mcpTestRootPassword   = ""
	mcpTestClientPassword = "passw0rd"
	doltServerHost        = "0.0.0.0"
	doltServerPort        = 3306
)

var ErrNoDatabaseConnection = errors.New("no database connection")

type testSuite struct {
	t                     *testing.T
	doltDatabaseParentDir string
	doltDatabaseDir       string
	dsn                   string
	testDb                *sql.DB
	doltServer            benchmark_runner.Server
}

func (s *testSuite) Ping() error {
	if s.testDb == nil {
		return ErrNoDatabaseConnection
	}
	return s.testDb.Ping()
}

func (s *testSuite) checkoutBranch(branchName string) error {
	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database before checking out a branch: %s", err.Error())
	}
	_, err = s.testDb.Exec(fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", branchName))
	return err
}

func (s *testSuite) createBranch(branchName string) error {
	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database before creating a branch: %s", err.Error())
	}
	_, err = s.testDb.Exec(fmt.Sprintf("CALL DOLT_BRANCH('-c', 'main', '%s');", branchName))
	return err
}

func (s *testSuite) deleteBranch(branchName string) error {
	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database before deleteing a branch: %s", err.Error())
	}
	_, err = s.testDb.Exec(fmt.Sprintf("CALL DOLT_BRANCH('-d', '%s');", branchName))
	return err
}

func (s *testSuite) addAndCommitChanges(commitMessage string) error {
	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database before adding and committing changes: %s", err.Error())
	}
	_, err = s.testDb.Exec(fmt.Sprintf("CALL DOLT_COMMIT('-Am', '%s');", commitMessage))
	return err
}

func (s *testSuite) exec(sql string) error {
	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database before executing sql: %s", err.Error())
	}
	_, err = s.testDb.Exec(sql)
	return err
}

func (s *testSuite) GlobalSetup() error {
	// todo: add the users and schema and shit here
	return nil
}

func (s *testSuite) Setup(newBranchName, setupSQL string) {
	if newBranchName == "" {
		s.t.Fatalf("no new branch name provided")
	}

	err := s.checkoutBranch("main")
	if err != nil {
		s.t.Fatalf("failed checkout main branch during test setup: %s", err.Error())
	}

	err = s.createBranch(newBranchName)
	if err != nil {
		s.t.Fatalf("failed checkout generated branch during test setup: %s", err.Error())
	}

	err = s.checkoutBranch(newBranchName)
	if err != nil {
		s.t.Fatalf("failed checkout main branch during test setup: %s", err.Error())
	}

	if setupSQL != "" {
		err = s.exec(setupSQL)
		if err != nil {
			s.t.Fatalf("failed setup database with setup sql: %s", err.Error())
		}

		err = s.addAndCommitChanges("add test setup changes")
		if err != nil {
			s.t.Fatalf("failed add and commit changes during test setup: %s", err.Error())
		}
	}
}

func (s *testSuite) Teardown(branchName string) {
	if branchName == "" {
		s.t.Fatalf("no new branch name provided")
	}

	err := s.Ping()
	if err != nil {
		s.t.Fatalf("failed to reach database: %s", err.Error())
	}

	err = s.checkoutBranch("main")
	if err != nil {
		s.t.Fatalf("failed checkout main branch during test teardown: %s", err.Error())
	}

	err = s.deleteBranch(branchName)
	if err != nil {
		s.t.Fatalf("failed delete branch during test teardown: %s", err.Error())
	}
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
		mcpTestRootUserName,
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

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true&parseTime=true", mcpTestRootUserName, mcpTestRootPassword, doltServerHost, doltServerPort, mcpTestDatabaseName)
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
