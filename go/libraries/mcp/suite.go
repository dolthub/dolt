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
	"golang.org/x/sync/errgroup"
)

const (
	mcpTestDatabaseName         = "test"
	mcpTestRootUserName         = "root"
	mcpTestMCPClientSQLUser     = "mcp-client-1"
	mcpTestMCPClientSQLPassword = "passw0rd"
	mcpTestRootPassword         = ""
	doltServerHost              = "0.0.0.0"
	doltServerPort              = 3306
	mcpServerPort               = 6900
)

var ErrNoDatabaseConnection = errors.New("no database connection")

type testSuite struct {
	t                     *testing.T
	doltDatabaseParentDir string
	doltDatabaseDir       string
	dsn                   string
	testDb                *sql.DB
	doltServer            benchmark_runner.Server
	mcpServer             Server
	mcpErrGroup           *errgroup.Group
	mcpErrGroupCancelFunc context.CancelFunc
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
		s.t.Fatalf("failed to reach database before deleting a branch: %s", err.Error())
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

func (s *testSuite) Setup(newBranchName, setupSQL string) {
	if newBranchName == "" {
		s.t.Fatalf("no new branch name provided")
	}

	err := s.exec(fmt.Sprintf("USE %s;", mcpTestDatabaseName))
	if err != nil {
		s.t.Fatalf("failed to use database during test setup: %s", err.Error())
	}

	err = s.checkoutBranch("main")
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

	err = s.exec(fmt.Sprintf("USE %s;", mcpTestDatabaseName))
	if err != nil {
		s.t.Fatalf("failed to use database during test setup: %s", err.Error())
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
		doltServer.Stop()
		return nil, err
	}

	err = testDb.PingContext(ctx)
	if err != nil {
		doltServer.Stop()
		testDb.Close()
		return nil, err
	}

	_, err = testDb.ExecContext(ctx, fmt.Sprintf("USE %s;", mcpTestDatabaseName))
	if err != nil {
		return nil, err
	}

	_, err = testDb.ExecContext(ctx, fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';", mcpTestMCPClientSQLUser, "%", mcpTestMCPClientSQLPassword))
	if err != nil {
		return nil, err
	}

	_, err = testDb.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%s';", mcpTestDatabaseName, mcpTestMCPClientSQLUser, "%"))
	if err != nil {
		return nil, err
	}

	// todo: do schema creation stuff here

	config := Config{
		Host:     doltServerHost,
		Port:     doltServerPort,
		User:     mcpTestMCPClientSQLUser,
		Password: mcpTestMCPClientSQLPassword,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	if err != nil {
		doltServer.Stop()
		testDb.Close()
		return nil, err
	}

	newCtx, cancelFunc := context.WithCancel(ctx)

	eg, egCtx := errgroup.WithContext(newCtx)

	eg.Go(func() error {
		mcpServer.ListenAndServe(egCtx, mcpServerPort)
		return nil
	})

	return &testSuite{
		dsn:                   dsn,
		doltServer:            doltServer,
		doltDatabaseParentDir: doltDatabaseParentDir,
		doltDatabaseDir:       doltDatabaseDir,
		testDb:                testDb,
		mcpServer:             mcpServer,
		mcpErrGroup:           eg,
		mcpErrGroupCancelFunc: cancelFunc,
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

	if s.mcpErrGroup != nil && s.mcpErrGroupCancelFunc != nil {
		s.mcpErrGroupCancelFunc()
		s.mcpErrGroup.Wait()
	}
}

