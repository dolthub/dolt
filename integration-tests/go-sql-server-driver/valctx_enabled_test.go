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

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// A short smoke test to assert that valctx is enabled for these tests.
func TestValctxEnabled(t *testing.T) {
	t.Parallel()

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("valctx_test")
	require.NoError(t, err)
	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
		LogMatches:  []string{
			"mysql_server caught panic:",
		},
	}
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "valctx_test"
	
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_test_valctx()")
	require.Error(t, err)
}
