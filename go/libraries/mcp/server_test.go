package mcp

import (
	"fmt"
	"testing"
	"github.com/stretchr/testify/require"
)

func TestMCPServer(t *testing.T) {
	t.Run("NewMCPServer create success", testNewMCPServerSuccess)
	t.Run("NewMCPServer create fails", testNewMCPServerFail)
}

func testNewMCPServerSuccess(t *testing.T) {
	t.Run("Valid Config", func(t *testing.T) {
		RunTest(t, "Without DSN", testSuccessWithoutDSN)
		RunTest(t, "With DSN", testSuccessWithDSN)
	})
}

func testNewMCPServerFail(t *testing.T) {
	t.Run("Invalid Config", func(t *testing.T) {
		RunTest(t, "Missing Host and DSN", testMissingHostAndDSN)
		RunTest(t, "Missing User and DSN", testMissingUserAndDSN)
		RunTest(t, "Missing Database Name and DSN", testMissingDatabaseNameAndDSN)
		RunTest(t, "Missing Port and DSN", testMissingPortAndDSN)
	})
}

func testSuccessWithDSN(suite *testSuite) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mcpTestMCPServerSQLUser, mcpTestMCPServerSQLPassword, doltServerHost, doltServerPort, mcpTestDatabaseName)
	config := Config{
		DSN: dsn,
	}

	mcpServer, err := NewMCPServer(config)
	require.NoError(suite.t, err)
	require.NotNil(suite.t, mcpServer)
}

func testSuccessWithoutDSN(suite *testSuite) {
	config := Config{
		Host:         doltServerHost,
		User:         mcpTestMCPServerSQLUser,
		Password:     mcpTestMCPServerSQLPassword,
		Port:         doltServerPort,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.NoError(suite.t, err)
	require.NotNil(suite.t, mcpServer)
}

func testMissingHostAndDSN(suite *testSuite) {
	config := Config{
		User:         mcpTestMCPServerSQLUser,
		Password:     mcpTestMCPServerSQLPassword,
		Port:         doltServerPort,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
	require.Equal(suite.t, err, ErrNoHostDefined)
	require.Nil(suite.t, mcpServer)
}

func testMissingUserAndDSN(suite *testSuite) {
	config := Config{
		Host:         doltServerHost,
		Password:     mcpTestMCPServerSQLPassword,
		Port:         doltServerPort,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
	require.Equal(suite.t, err, ErrNoUserDefined)
	require.Nil(suite.t, mcpServer)
}

func testMissingDatabaseNameAndDSN(suite *testSuite) {
	config := Config{
		Host:         doltServerHost,
		User:         mcpTestMCPServerSQLUser,
		Password:     mcpTestMCPServerSQLPassword,
		Port:         doltServerPort,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
	require.Equal(suite.t, err, ErrNoDatabaseNameDefined)
	require.Nil(suite.t, mcpServer)
}

func testMissingPortAndDSN(suite *testSuite) {
	config := Config{
		Host:         doltServerHost,
		User:         mcpTestMCPServerSQLUser,
		Password:     mcpTestMCPServerSQLPassword,
		DatabaseName: mcpTestDatabaseName,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
	require.Equal(suite.t, err, ErrNoPortDefined)
	require.Nil(suite.t, mcpServer)
}

// func testLiveServerPing(suite *testSuite) {
// 	config := Config{
// 		Host:         doltServerHost,
// 		User:         mcpTestClientUserName,
// 		Password:     mcpTestClientPassword,
// 		Port:         doltServerPort,
// 		DatabaseName: mcpTestDatabaseName,
// 	}
//
// 	mcpServer, err := NewMCPServer(config)
// 	require.NoError(suite.t, err)
// 	require.NotNil(suite.t, mcpServer)
// }

