// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlserver

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
)

// validateAndPrepareMCP performs coherence checks for MCP options and fails fast
// when the specified port is unavailable. It also sets DOLT_ROOT_HOST dynamically
// when MCP is enabled and the env var is not already set.
func validateAndPrepareMCP(
	serverConfig servercfg.ServerConfig,
	mcpPort *int,
	mcpUser *string,
	mcpPassword *string,
	mcpDatabase *string,
) error {
	// If any MCP-related arg is supplied without --mcp-port, error
	if mcpPort == nil {
		if mcpUser != nil && *mcpUser != "" {
			return fmt.Errorf("--%s requires --%s to be set", mcpUserFlag, mcpPortFlag)
		}
		if mcpPassword != nil && *mcpPassword != "" {
			return fmt.Errorf("--%s requires --%s to be set", mcpPasswordFlag, mcpPortFlag)
		}
		if mcpDatabase != nil && *mcpDatabase != "" {
			return fmt.Errorf("--%s requires --%s to be set", mcpDatabaseFlag, mcpPortFlag)
		}
		return nil
	}

	// --mcp-user is REQUIRED when --mcp-port is provided
	if mcpUser == nil || *mcpUser == "" {
		return fmt.Errorf("--%s is required when --%s is specified", mcpUserFlag, mcpPortFlag)
	}

	// Range and conflict checks
	if *mcpPort <= 0 || *mcpPort > 65535 {
		return fmt.Errorf("invalid value for --%s '%d'", mcpPortFlag, *mcpPort)
	}
	if serverConfig.Port() == *mcpPort {
		return fmt.Errorf("--%s must differ from --%s (both set to %d)", mcpPortFlag, portFlag, *mcpPort)
	}

	// If MCP is enabled and no explicit root host override exists, set DOLT_ROOT_HOST dynamically
	// to the SQL listener host, defaulting to 127.0.0.1 when unspecified or wildcard.
	if _, ok := os.LookupEnv(dconfig.EnvDoltRootHost); !ok {
		hostForRoot := serverConfig.Host()
		if hostForRoot == "" || hostForRoot == "0.0.0.0" || hostForRoot == "::" {
			hostForRoot = "127.0.0.1"
		}
		_ = os.Setenv(dconfig.EnvDoltRootHost, hostForRoot)
	}

	// Preflight port availability to fail fast before starting controller
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(*mcpPort))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("MCP port %d already in use: %v", *mcpPort, err)
	}
	_ = l.Close()
	return nil
}
