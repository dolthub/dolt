package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPServer(t *testing.T) {
	t.Run("NewMCPServer success", testNewMCPServerSuccess)
	t.Run("NewMCPServer fails", testNewMCPServerFail)
}

func testNewMCPServerSuccess(t *testing.T) {
	t.Run("Valid Config", func(t *testing.T) {
		RunTest(t, "Without DSN", testSuccessWithoutDSN)
	})
}

func testNewMCPServerFail(t *testing.T) {
	t.Run("Invalid Config", func(t *testing.T) {
		RunTest(t, "Missing Host and DSN", testMissingHostAndDSN)
		// RunTest(t, "Missing User and DSN", testMissingUserAndDSN)
		// RunTest(t, "Missing Database Name and DSN", testMissingDatabaseNameAndDSN)
		// RunTest(t, "Missing Port and DSN", testMissingPortAndDSN)
	})
	// t.Run("Invalid SQL user", nil)
	// t.Run("Invalid SQL password", nil)
	// t.Run("Invalid SQL grants", nil)
}

func testSuccessWithoutDSN(suite *testSuite) {
	config := Config{
		Host:         doltServerHost,
		User:         mcpTestClientUserName,
		Password:     mcpTestClientPassword,
		Port:         doltServerPort,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.NoError(suite.t, err)
	require.NotNil(suite.t, mcpServer)
}

func testMissingHostAndDSN(suite *testSuite) {
	config := Config{
		User:         mcpTestClientUserName,
		Password:     mcpTestClientPassword,
		Port:         doltServerPort,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
	require.Equal(suite.t, err, ErrNoHostDefined)
	require.Nil(suite.t, mcpServer)
}

// func testMissingUserAndDSN(suite *testSuite) {
//
// }
//
// func testMissingDatabaseNameAndDSN(suite *testSuite) {
//
// }
//
// func testMissingPortAndDSN(suite *testSuite) {
//
// }
//
