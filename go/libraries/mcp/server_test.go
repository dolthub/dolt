package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPServer(t *testing.T) {
	// t.Run("NewMCPServer success", nil)
	t.Run("NewMCPServer fails", testNewMCPServerFails)
}

func testNewMCPServerFails(t *testing.T) {
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

func testMissingHostAndDSN(suite *testSuite) {
	config := Config{
		User: mcpTestUserName,	
		Password: mcpTestPassword,
		Port: doltServerPort,
	}

	mcpServer, err := NewMCPServer(config)
	require.Error(suite.t, err)
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
