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

package enginetest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
)

// TestBranchDeleteRacesSwitch races a session switching onto a branch against another deleting or renaming it without
// --force. One of them must fail: the switch, because the branch is gone, or the delete, because the branch is in use.
func TestBranchDeleteRacesSwitch(t *testing.T) {
	dEnv, sc, serverConfig := startServer(t, true, "", "")
	require.NoError(t, sc.WaitForStart())
	defer dEnv.Close()
	defer func() {
		sc.Stop()
		require.NoError(t, sc.WaitForStop())
	}()

	ctx := context.Background()
	db, err := gosql.Open("mysql", servercfg.ConnectionString(serverConfig, "dolt"))
	require.NoError(t, err)
	defer db.Close()
	// Close the connections a test is done with, rather than handing them to the next one with their state.
	db.SetMaxIdleConns(0)

	deleter, err := db.Conn(ctx)
	require.NoError(t, err)
	defer deleter.Close()
	cleanUp := func(branch string) {
		for _, b := range []string{branch, branch + "_moved"} {
			// Either may not exist.
			_, _ = deleter.ExecContext(ctx, fmt.Sprintf("call dolt_branch('-D', '%s')", b))
		}
	}

	const iterations = 200
	for _, switchStmt := range []string{"call dolt_checkout('%[1]s')", "use `dolt/%[1]s`"} {
		for _, deleteStmt := range []string{"call dolt_branch('-d', '%[1]s')", "call dolt_branch('-m', '%[1]s', '%[1]s_moved')"} {
			t.Run(switchStmt+" vs "+deleteStmt, func(t *testing.T) {
				// Without the race, deleting a branch another session switched onto fails.
				_, err := deleter.ExecContext(ctx, "call dolt_branch('seq')")
				require.NoError(t, err)
				user, err := db.Conn(ctx)
				require.NoError(t, err)
				_, err = user.ExecContext(ctx, fmt.Sprintf(switchStmt, "seq"))
				require.NoError(t, err)
				_, err = deleter.ExecContext(ctx, fmt.Sprintf(deleteStmt, "seq"))
				require.ErrorContains(t, err, "in use in other sessions")
				require.NoError(t, user.Close())
				cleanUp("seq")

				var switched, deleted, both int
				for i := 0; i < iterations; i++ {
					branch := fmt.Sprintf("b%d", i)
					_, err := deleter.ExecContext(ctx, fmt.Sprintf("call dolt_branch('%s')", branch))
					require.NoError(t, err)
					user, err := db.Conn(ctx)
					require.NoError(t, err)

					var switchErr, deleteErr error
					var wg sync.WaitGroup
					start := make(chan struct{})
					wg.Add(2)
					go func() {
						defer wg.Done()
						<-start
						_, switchErr = user.ExecContext(ctx, fmt.Sprintf(switchStmt, branch))
					}()
					go func() {
						defer wg.Done()
						<-start
						_, deleteErr = deleter.ExecContext(ctx, fmt.Sprintf(deleteStmt, branch))
					}()
					close(start)
					wg.Wait()

					switch {
					case switchErr == nil && deleteErr == nil:
						both++
					case switchErr == nil:
						switched++
						assert.ErrorContains(t, deleteErr, "in use in other sessions")
					case deleteErr == nil:
						deleted++
					default:
						t.Errorf("both failed: switch: %v, delete: %v", switchErr, deleteErr)
					}
					require.NoError(t, user.Close())
					cleanUp(branch)
				}
				t.Logf("switch won %d, delete won %d", switched, deleted)
				assert.Zero(t, both, "a session switched onto a branch that another session deleted")
			})
		}
	}
}
