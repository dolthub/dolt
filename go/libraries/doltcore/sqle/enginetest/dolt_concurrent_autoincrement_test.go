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
	gosql "database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
)

func TestConcurrentAutoIncrementInserts(t *testing.T) {
	const (
		dbName      = "dolt_concurrency_repro"
		tableName   = "global_audit_log"
		threadCount = 100
	)

	dEnv, sc, serverConfig := startServer(t, true, "", "")
	require.NoError(t, sc.WaitForStart())
	defer dEnv.Close()
	defer func() {
		sc.Stop()
		require.NoError(t, sc.WaitForStop())
	}()

	// openDB opens a connection pool to the named database. multiStatements is enabled so the
	// batched setup below can be executed verbatim.
	openDB := func(database string) *gosql.DB {
		dsn := servercfg.ConnectionString(serverConfig, database)
		if strings.Contains(dsn, "?") {
			dsn += "&multiStatements=true"
		} else {
			dsn += "?multiStatements=true"
		}
		db, err := gosql.Open("mysql", dsn)
		require.NoError(t, err)
		return db
	}

	{
		conn := openDB("dolt")
		_, err := conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	{
		conn := openDB(dbName)
		_, err := conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf(
			"CREATE TABLE `%s` (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, message TEXT)", tableName))
		require.NoError(t, err)
	}

	var (
		mu       sync.Mutex
		failures []string
		wg       sync.WaitGroup
	)
	for i := 1; i <= threadCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn := openDB(dbName)
			defer conn.Close()
			_, err := conn.Exec(
				fmt.Sprintf("INSERT INTO `%s` (message) VALUES (?)", tableName),
				fmt.Sprintf("message %d", i))
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%T: %v", err, err))
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	report := openDB(dbName)
	defer report.Close()

	var rowCount, distinctIds int
	var maxID gosql.NullInt64
	require.NoError(t, report.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)).Scan(&rowCount))
	require.NoError(t, report.QueryRow(fmt.Sprintf("SELECT COUNT(DISTINCT id) FROM `%s`", tableName)).Scan(&distinctIds))
	require.NoError(t, report.QueryRow(fmt.Sprintf("SELECT MAX(id) FROM `%s`", tableName)).Scan(&maxID))

	t.Logf("Attempted inserts : %d", threadCount)
	t.Logf("Rows persisted    : %d", rowCount)
	t.Logf("Distinct ids      : %d", distinctIds)
	t.Logf("Max id            : %v", maxID.Int64)
	t.Logf("Failed inserts    : %d", len(failures))

	groups := map[string]int{}
	for _, f := range failures {
		groups[f]++
	}
	grouped := make([]string, 0, len(groups))
	for msg := range groups {
		grouped = append(grouped, msg)
	}
	sort.Slice(grouped, func(i, j int) bool { return groups[grouped[i]] > groups[grouped[j]] })
	for _, msg := range grouped {
		t.Logf("  %dx  %s", groups[msg], msg)
	}

	require.Emptyf(t, failures, "expected no failed inserts, got %d", len(failures))
	require.Equalf(t, distinctIds, rowCount,
		"duplicate auto-increment ids detected: %d rows but only %d distinct ids", rowCount, distinctIds)
	require.Equalf(t, threadCount, rowCount, "expected %d rows persisted, got %d", threadCount, rowCount)
}
