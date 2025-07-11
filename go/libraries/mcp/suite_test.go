package mcp

import (
	"fmt"
	"testing"

	"context"

	"os"
	"os/exec"

	"github.com/google/uuid"
)

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

	err = suite.GlobalSetup()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to perform test suite global setup: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	teardownMCPDoltServerTestSuite(suite)
	os.RemoveAll(suite.doltDatabaseParentDir)

	os.Exit(code)
}

func generateTestBranchName() string {
	return uuid.NewString()
}

func RunTest(t *testing.T, testName string, testFunc func(s *testSuite)) {
	t.Run(testName, func(t *testing.T) {
		if suite == nil {
			t.Fatalf("no test suite")
		}
		suite.t = t
		testBranchName := generateTestBranchName()
		suite.Setup(testBranchName, "")
		defer suite.Teardown(testBranchName)
		testFunc(suite)
	})
}

func RunTestWithSetupSQL(t *testing.T, testName, setupSQL string, testFunc func(s *testSuite)) {
	t.Run(testName, func(t *testing.T) {
		if suite == nil {
			t.Fatalf("no test suite")
		}
		suite.t = t
		testBranchName := generateTestBranchName()
		suite.Setup(testBranchName, setupSQL)
		defer suite.Teardown(testBranchName)
		testFunc(suite)
	})
}
