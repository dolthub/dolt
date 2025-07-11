package mcp

import (
	"fmt"
	"testing"

	"context"

	"os"
	"os/exec"
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
	defer func() {
		teardownMCPDoltServerTestSuite(suite)
		os.RemoveAll(suite.doltDatabaseParentDir)
	}()

	// todo: seed the database with test users and schema
	code := m.Run()

	os.Exit(code)
}

