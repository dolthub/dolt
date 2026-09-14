// Copyright 2023 Dolthub, Inc.
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

package binlogreplication

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmsbinlogreplication "github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

// TestWildcardPatternMatches verifies MySQL's byte-oriented wildcard and escape semantics.
func TestWildcardPatternMatches(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		value    string
		expected bool
	}{
		{name: "percent", pattern: "db.t%", value: "db.table", expected: true},
		{name: "single byte", pattern: "db._x", value: "db.ax", expected: true},
		{name: "underscore is not a rune", pattern: "db._x", value: "db.éx", expected: false},
		{name: "two underscores match two byte rune", pattern: "db.__x", value: "db.éx", expected: true},
		{name: "literal percent", pattern: `db.literal\%`, value: "db.literal%", expected: true},
		{name: "literal underscore", pattern: `db.literal\_`, value: "db.literal_", expected: true},
		{name: "terminal backslash", pattern: `db.table\`, value: `db.table\`, expected: true},
		{name: "requires complete match", pattern: "db.t_", value: "db.table", expected: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, wildcardPatternMatches(compileWildcardPattern(test.pattern), test.value))
		})
	}
}

// TestWildcardFilterCasePolicy verifies matching follows the replica's lower_case_table_names policy.
func TestWildcardFilterCasePolicy(t *testing.T) {
	caseSensitive, err := compileTablePatterns([]string{"CaseDB.Mixed%"}, false)
	require.NoError(t, err)
	require.True(t, tablePatternsMatch(caseSensitive, "CaseDB.MixedName"))
	require.False(t, tablePatternsMatch(caseSensitive, "casedb.mixedname"))

	caseInsensitive, err := compileTablePatterns([]string{"CaseDB.Mixed%"}, true)
	require.NoError(t, err)
	require.True(t, tablePatternsMatch(caseInsensitive, "casedb.mixedname"))
}

// TestCompileTablePatternsValidation verifies MySQL's qualified-pattern validation boundary.
func TestCompileTablePatternsValidation(t *testing.T) {
	for _, pattern := range []string{".table", "db.", "db.table.extra", "db..table"} {
		_, err := compileTablePatterns([]string{pattern}, false)
		require.NoError(t, err, pattern)
	}

	for _, pattern := range []string{"", "missing_dot"} {
		_, err := compileTablePatterns([]string{pattern}, false)
		require.Error(t, err, pattern)
	}
}

// BenchmarkWildcardTableFilterMatches measures the row-event wildcard hot path with compiled patterns.
func BenchmarkWildcardTableFilterMatches(b *testing.B) {
	filters, err := compileTablePatterns([]string{"sales.orders_%", "archive.20__%", `db.literal\%`, "tenant%.events"}, false)
	require.NoError(b, err)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tablePatternsMatch(filters, "tenant42.events")
	}
}

// TestTableFilterPrecedence verifies MySQL's exact and wildcard early-return ordering.
func TestTableFilterPrecedence(t *testing.T) {
	wildDo, err := compileTablePatterns([]string{"db.wild_both%", "db.from_wild_do"}, false)
	require.NoError(t, err)
	wildIgnore, err := compileTablePatterns([]string{"db.wild_both%", "db.from_wild_ignore"}, false)
	require.NoError(t, err)

	filters := &filterConfiguration{
		doTables: map[string]map[string]struct{}{
			"db": {"exact_both": {}},
		},
		ignoreTables: map[string]map[string]struct{}{
			"db": {"exact_both": {}, "from_exact_ignore": {}},
		},
		wildDoTables:     wildDo,
		wildIgnoreTables: wildIgnore,
	}

	ctx := sql.NewEmptyContext()
	tests := []struct {
		table       *mysql.TableMap
		filteredOut bool
	}{
		{table: &mysql.TableMap{Database: "db", Name: "exact_both"}, filteredOut: false},
		{table: &mysql.TableMap{Database: "db", Name: "from_exact_ignore"}, filteredOut: true},
		{table: &mysql.TableMap{Database: "db", Name: "wild_both_a"}, filteredOut: false},
		{table: &mysql.TableMap{Database: "db", Name: "from_wild_ignore"}, filteredOut: true},
		{table: &mysql.TableMap{Database: "db", Name: "unmatched"}, filteredOut: true},
		{table: &mysql.TableMap{Database: "other", Name: "exact_both"}, filteredOut: true},
	}

	for _, test := range tests {
		t.Run(test.table.Database+"."+test.table.Name, func(t *testing.T) {
			require.Equal(t, test.filteredOut, filters.isTableFilteredOut(ctx, test.table))
		})
	}
}

// TestConcurrentReplicationFilterUpdates verifies successful updates to different filter types are not lost.
func TestConcurrentReplicationFilterUpdates(t *testing.T) {
	controller := newDoltBinlogReplicaController()
	ctx := sql.NewEmptyContext()
	doOption := *gmsbinlogreplication.NewReplicationOption("REPLICATE_DO_TABLE", []sql.UnresolvedTable{plan.NewUnresolvedTable("included", "db")})
	ignoreOption := *gmsbinlogreplication.NewReplicationOption("REPLICATE_IGNORE_TABLE", []sql.UnresolvedTable{plan.NewUnresolvedTable("ignored", "db")})

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, option := range []gmsbinlogreplication.ReplicationOption{doOption, ignoreOption} {
		wg.Add(1)
		go func(option gmsbinlogreplication.ReplicationOption) {
			defer wg.Done()
			<-start
			errs <- controller.SetReplicationFilterOptions(ctx, []gmsbinlogreplication.ReplicationOption{option})
		}(option)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	doTables, ignoreTables, _, _ := controller.filters.tableFilters()
	require.Equal(t, []string{"db.included"}, doTables)
	require.Equal(t, []string{"db.ignored"}, ignoreTables)
}

// TestBinlogReplicationFilters_ignoreTablesOnly tests that the ignoreTables replication
// filtering option is correctly applied and honored.
func TestBinlogReplicationFilters_ignoreTablesOnly(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicaSystemVars)
	h.startReplicationAndCreateTestDb(h.mySqlPort)
	h.replicaDatabase.MustExec("STOP REPLICA;")

	// Ignore replication events for db01.t2. Also tests that the first filter setting is overwritten by the second.
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);")
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t2);")
	h.replicaDatabase.MustExec("START REPLICA;")

	// Assert that status shows replication filters
	status := h.showReplicaStatus()
	require.Equal(t, "db01.t2", status["Replicate_Ignore_Table"])
	require.Equal(t, "", status["Replicate_Do_Table"])

	// Make changes on the primary
	h.primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	h.primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	h.primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	h.primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	h.waitForReplicaToCatchUp()

	// Verify that all changes from t1 were applied on the replica
	rows, err := h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that no changes from t2 were applied on the replica
	rows, err = h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_doTablesOnly tests that the doTables replication
// filtering option is correctly applied and honored.
func TestBinlogReplicationFilters_doTablesOnly(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicaSystemVars)
	h.startReplicationAndCreateTestDb(h.mySqlPort)
	h.replicaDatabase.MustExec("STOP REPLICA;")

	// Do replication events for db01.t1. Also tests that the first filter setting is overwritten by the second.
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(db01.t2);")
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(db01.t1);")
	h.replicaDatabase.MustExec("START REPLICA;")

	// Assert that status shows replication filters
	status := h.showReplicaStatus()
	require.Equal(t, "db01.t1", status["Replicate_Do_Table"])
	require.Equal(t, "", status["Replicate_Ignore_Table"])

	// Make changes on the primary
	h.primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	h.primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	h.primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	h.primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	h.waitForReplicaToCatchUp()

	// Verify that all changes from t1 were applied on the replica
	rows, err := h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that no changes from t2 were applied on the replica
	rows, err = h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_doTablesAndIgnoreTables tests MySQL's exact-do-before-exact-ignore precedence.
func TestBinlogReplicationFilters_doTablesAndIgnoreTables(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicaSystemVars)
	h.startReplicationAndCreateTestDb(h.mySqlPort)
	h.replicaDatabase.MustExec("STOP REPLICA;")

	// Do replication events for db01.t1, and db01.t2
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(db01.t1, db01.t2);")
	// Also ignore db01.t2; the earlier exact do match takes precedence.
	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t2);")
	h.replicaDatabase.MustExec("START REPLICA;")

	// Assert that replica status shows replication filters
	status := h.showReplicaStatus()
	require.True(t, status["Replicate_Do_Table"] == "db01.t1,db01.t2" ||
		status["Replicate_Do_Table"] == "db01.t2,db01.t1")
	require.Equal(t, "db01.t2", status["Replicate_Ignore_Table"])

	// Make changes on the primary
	h.primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	h.primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	h.primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	h.primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	h.primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	h.waitForReplicaToCatchUp()

	// Verify that all changes from t1 were applied on the replica
	rows, err := h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that exact do wins over exact ignore and all changes from t2 were applied.
	rows, err = h.replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_wildTables verifies wildcard matching, precedence, status, and atomic updates.
func TestBinlogReplicationFilters_wildTables(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicaSystemVars)
	h.startReplicationAndCreateTestDb(h.mySqlPort)

	_, err := h.replicaDatabase.Exec("CHANGE REPLICATION FILTER REPLICATE_WILD_DO_TABLE=('db01.%');")
	require.Error(t, err)
	require.ErrorContains(t, err, "Error 3085")
	require.ErrorContains(t, err, "running replica sql thread")
	h.replicaDatabase.MustExec("STOP REPLICA;")

	h.replicaDatabase.MustExec(`CHANGE REPLICATION FILTER
		REPLICATE_DO_TABLE=(db01.exact_both),
		REPLICATE_IGNORE_TABLE=(db01.exact_both, db01.exact_ignore),
		REPLICATE_WILD_DO_TABLE=('db01.exact_ignore', 'db01.wild_both%', 'db01.literal\\%', 'db01.literal\\_'),
		REPLICATE_WILD_IGNORE_TABLE=('db01.wild_both%', 'db01.wild_ignore');`)
	h.replicaDatabase.MustExec("START REPLICA;")

	status := h.showReplicaStatus()
	require.Equal(t, `db01.exact_ignore,db01.wild_both%,db01.literal\%,db01.literal\_`, status["Replicate_Wild_Do_Table"])
	require.Equal(t, "db01.wild_both%,db01.wild_ignore", status["Replicate_Wild_Ignore_Table"])

	tables := []string{"exact_both", "exact_ignore", "wild_both_a", "wild_ignore", "unmatched", "`literal%`", "`literal_`"}
	for _, table := range tables {
		h.primaryDatabase.MustExec(fmt.Sprintf("CREATE TABLE db01.%s (pk INT PRIMARY KEY);", table))
		h.primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.%s VALUES (1);", table))
	}
	h.primaryDatabase.MustExec("CREATE DATABASE db02;")
	h.primaryDatabase.MustExec("CREATE TABLE db02.exact_both (pk INT PRIMARY KEY);")
	h.primaryDatabase.MustExec("INSERT INTO db02.exact_both VALUES (1);")

	h.waitForReplicaToCatchUp()

	// Table filters currently apply only to row events. DDL is delivered in QueryEvents, so the
	// filtered db02 table is created even though its subsequent row event is excluded by the do filters.
	rows, err := h.replicaDatabase.Queryx("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = 'db02' AND table_name = 'exact_both'")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["count"])
	require.NoError(t, rows.Close())

	expectedCounts := map[string]string{
		"db01.exact_both":   "1",
		"db01.exact_ignore": "0",
		"db01.wild_both_a":  "1",
		"db01.wild_ignore":  "0",
		"db01.unmatched":    "0",
		"db01.`literal%`":   "1",
		"db01.`literal_`":   "1",
		"db02.exact_both":   "0",
	}
	for table, expected := range expectedCounts {
		rows, err := h.replicaDatabase.Queryx("SELECT COUNT(*) AS count FROM " + table)
		require.NoError(t, err)
		row := convertMapScanResultToStrings(readNextRow(t, rows))
		require.Equal(t, expected, row["count"], table)
		require.NoError(t, rows.Close())
	}

	h.replicaDatabase.MustExec("STOP REPLICA;")
	_, err = h.replicaDatabase.Queryx(`CHANGE REPLICATION FILTER
		REPLICATE_WILD_IGNORE_TABLE=('replacement.valid'),
		REPLICATE_WILD_DO_TABLE=('missing_dot');`)
	require.Error(t, err)
	require.ErrorContains(t, err, "Error 3067")
	require.ErrorContains(t, err, "Supplied filter list contains a value which is not in the required format 'db_pattern.table_pattern'")
	status = h.showReplicaStatus()
	require.Equal(t, `db01.exact_ignore,db01.wild_both%,db01.literal\%,db01.literal\_`, status["Replicate_Wild_Do_Table"])
	require.Equal(t, "db01.wild_both%,db01.wild_ignore", status["Replicate_Wild_Ignore_Table"])

	h.replicaDatabase.MustExec(`CHANGE REPLICATION FILTER
		REPLICATE_WILD_DO_TABLE=('first.value'),
		REPLICATE_WILD_DO_TABLE=('second.value');`)
	status = h.showReplicaStatus()
	require.Equal(t, "second.value", status["Replicate_Wild_Do_Table"])

	h.replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_WILD_DO_TABLE=();")
	status = h.showReplicaStatus()
	require.Equal(t, "", status["Replicate_Wild_Do_Table"])
}

// TestBinlogReplicationFilters_errorCases test returned errors for various error cases.
func TestBinlogReplicationFilters_errorCases(t *testing.T) {
	h := newHarness(t)
	h.startSqlServers()

	// All tables must be qualified with a database
	_, err := h.replicaDatabase.Queryx("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(t1);")
	require.Error(t, err)
	require.ErrorContains(t, err, "no database specified for table")

	_, err = h.replicaDatabase.Queryx("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(t1);")
	require.Error(t, err)
	require.ErrorContains(t, err, "no database specified for table")
}
