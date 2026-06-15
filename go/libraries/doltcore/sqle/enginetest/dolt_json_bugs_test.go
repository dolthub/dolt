package enginetest

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
)

// TestConcurrentJsonReads reproduces Bug 3 from GitHub issue #11210: concurrent JSON reads
// return corrupted data. A data race exists in unescapeHTMLCodepoints (blob_builder.go) which
// modifies the backing slice in place while other goroutines may be reading it.
// Run with -race to reliably detect the race; without -race, corruption is non-deterministic.
func TestConcurrentJsonReads(t *testing.T) {
	const (
		dbName     = "dolt_json_concurrent"
		tableName  = "repro_test"
		numWorkers = 8
		numOps     = 50
		ctrlCharN  = 666
	)

	dEnv, sc, serverConfig := startServer(t, true, "", "")
	require.NoError(t, sc.WaitForStart())
	defer dEnv.Close()
	defer func() {
		sc.Stop()
		require.NoError(t, sc.WaitForStop())
	}()

	dsn := servercfg.ConnectionString(serverConfig, dbName)
	if strings.Contains(dsn, "?") {
		dsn += "&multiStatements=true"
	} else {
		dsn += "?multiStatements=true"
	}

	setupDSN := servercfg.ConnectionString(serverConfig, "")
	if strings.Contains(setupDSN, "?") {
		setupDSN += "&multiStatements=true"
	} else {
		setupDSN += "?multiStatements=true"
	}
	setupDB, err := gosql.Open("mysql", setupDSN)
	require.NoError(t, err)
	defer setupDB.Close()

	_, err = setupDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	require.NoError(t, err)
	_, err = setupDB.Exec("USE " + dbName)
	require.NoError(t, err)

	_, err = setupDB.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id INT AUTO_INCREMENT PRIMARY KEY,
		label TEXT NOT NULL,
		data_json JSON,
		data_text LONGTEXT
	)`, tableName))
	require.NoError(t, err)

	// Insert rows with and without HTML-like codepoints (triggers unescapeHTMLCodepoints path)
	_, err = setupDB.Exec(fmt.Sprintf(`INSERT INTO %s (label, data_json, data_text) VALUES ('small', '{"d":"hello"}', '{"d":"hello"}')`, tableName))
	require.NoError(t, err)
	largeJSON := fmt.Sprintf(`{"d":"%s"}`, strings.Repeat(`\u000b`, ctrlCharN))
	_, err = setupDB.Exec(fmt.Sprintf(`INSERT INTO %s (label, data_json, data_text) VALUES ('large', '%s', '{"d":"hello"}')`, tableName, largeJSON))
	require.NoError(t, err)

	// Worker pool: each worker opens its own *sql.DB (separate connection pool)
	type result struct {
		garbled int
		ok      int
	}
	results := make(chan result, numWorkers)
	work := make(chan int, numOps)
	for i := 0; i < numOps; i++ {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := gosql.Open("mysql", dsn)
			if err != nil {
				t.Errorf("worker open: %v", err)
				return
			}
			defer db.Close()

			var garbled, ok int
			for range work {
				rows, err := db.Query(fmt.Sprintf("SELECT label, CAST(data_json AS CHAR) AS raw_json FROM %s ORDER BY id", tableName))
				if err != nil {
					t.Errorf("worker query: %v", err)
					continue
				}
				for rows.Next() {
					var label, rawJSON string
					if err := rows.Scan(&label, &rawJSON); err != nil {
						t.Errorf("worker scan: %v", err)
						continue
					}
					if rawJSON == "" || rawJSON == "null" || !strings.HasPrefix(rawJSON, "{") {
						garbled++
					} else {
						ok++
					}
				}
				rows.Close()
			}
			results <- result{garbled, ok}
		}()
	}
	wg.Wait()
	close(results)

	totalGarbled, totalOK := 0, 0
	for r := range results {
		totalGarbled += r.garbled
		totalOK += r.ok
	}
	t.Logf("JSON reads: %d OK, %d garbled/null", totalOK, totalGarbled)

	assert.Equal(t, 0, totalGarbled, "concurrent JSON reads returned corrupted data (Bug 3)")
	assert.Greater(t, totalOK, 0, "at least some JSON reads should succeed")
}
