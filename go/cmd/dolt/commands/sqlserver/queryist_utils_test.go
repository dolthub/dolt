// Copyright 2026 Dolthub, Inc.
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
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func getRefusedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestBuildConnectionStringQueryistNamesTargetOnConnectFailure(t *testing.T) {
	port := getRefusedPort(t)

	tests := []struct {
		name string
		host string
	}{
		{
			name: "ipv4 loopback",
			host: "127.0.0.1",
		},
		{
			name: "localhost",
			host: "localhost",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := filesys.EmptyInMemFS(t.TempDir())
			creds := &cli.UserPassword{Username: "root", Password: "", Specified: false}
			apr := argparser.NewEmptyResults()

			lateBind, err := BuildConnectionStringQueryist(context.Background(), fs, creds, apr,
				tc.host, port, QueryistTLSMode_Disabled, "mydb", "Test User", "test@test.com")
			require.NoError(t, err)

			_, err = lateBind(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to connect to the dolt sql-server at "+tc.host+":"+strconv.Itoa(port))
		})
	}
}
