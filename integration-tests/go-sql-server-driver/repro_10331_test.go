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

package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestRegression10331 tests for the checksum error caused by concurrent executions of `dolt sql`
// while dolt sql-server is processing writes.
//
// https://github.com/dolthub/dolt/issues/10331
func TestRegression10331(t *testing.T) {
	t.Parallel()
	// This test is not 100% reliable at detecting errors quickly, so we run it a few times.
	const testCount = 4
	for i := range testCount {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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

			repo, err := rs.MakeRepo("regression_10331_test")
			require.NoError(t, err)

			srvSettings := &driver.Server{
				Args:        []string{"-P", `{{get_port "server_port"}}`},
				DynamicPort: "server_port",
			}
			server := MakeServer(t, repo, srvSettings, &ports)
			server.DBName = "regression_10331_test"

			db, err := server.DB(driver.Connection{User: "root"})
			require.NoError(t, err)
			defer db.Close()

			ctx := t.Context()

			func() {
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()

				// Create table and initial data.
				_, err = conn.ExecContext(ctx, "CREATE TABLE data (id INT PRIMARY KEY AUTO_INCREMENT, val LONGTEXT)")
				require.NoError(t, err)
				for i := 0; i <= 50; i++ {
					var bs [10240]byte
					rand.Read(bs[:])
					data := base64.StdEncoding.EncodeToString(bs[:])
					_, err = conn.ExecContext(ctx, "INSERT INTO data (val) VALUES (?)", data)
					require.NoError(t, err)
				}
				_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'init with data')")
				require.NoError(t, err)
			}()

			eg, ctx := errgroup.WithContext(ctx)
			start := time.Now()

			var successfulWrites, successfulReads int32
			var errWrites, errReads int32
			const numWriters = 8
			const numReaders = 16
			const testDuration = 8 * time.Second
			for i := range numWriters {
				eg.Go(func() error {
					var bs [1024]byte
					rand.Read(bs[:])
					data := base64.StdEncoding.EncodeToString(bs[:])
					j := 0
					for {
						if time.Since(start) > testDuration {
							return nil
						}
						if ctx.Err() != nil {
							return nil
						}
						out, err := func() (string, error) {
							cmd := repo.DoltCmd("sql", "-r", "csv", "-q", fmt.Sprintf("INSERT INTO data (val) VALUES ('%s'); CALL DOLT_COMMIT('--allow-empty', '-Am', 'w%d c%d')", data, i, j))
							out, err := cmd.CombinedOutput()
							return string(out), err
						}()
						if err != nil {
							t.Logf("error writing, %v", err)
							atomic.AddInt32(&errWrites, 1)
							if strings.Contains(out, "checksum") || strings.Contains(out, " EOF") {
								return fmt.Errorf("error writing values %d: %s, %w", i, out, err)
							}
						} else {
							atomic.AddInt32(&successfulWrites, 1)
						}
						j += 1
					}
				})
			}
			for i := range numReaders {
				eg.Go(func() error {
					j := 0
					for {
						if time.Since(start) > testDuration {
							return nil
						}
						if ctx.Err() != nil {
							return nil
						}
						out, err := func() (string, error) {
							cmd := repo.DoltCmd("sql", "-r", "csv", "-q", "SELECT COUNT(*) FROM data; SELECT * FROM dolt_log LIMIT 20")
							out, err := cmd.CombinedOutput()
							return string(out), err
						}()
						if err != nil {
							t.Logf("error reading, %v", err)
							atomic.AddInt32(&errReads, 1)
							if strings.Contains(out, "checksum") || strings.Contains(out, " EOF") {
								return fmt.Errorf("READER error %d: %s, %w", i, out, err)
							}
						} else {
							atomic.AddInt32(&successfulReads, 1)
						}
						j += 1
					}
				})
			}

			require.NoError(t, eg.Wait())
			t.Logf("err writes: %d, err reads: %d", errWrites, errReads)
			t.Logf("successful writes: %d, successful reads: %d", successfulWrites, successfulReads)
		})
	}
}
