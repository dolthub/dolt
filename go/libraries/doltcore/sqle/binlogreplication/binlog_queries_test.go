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

package binlogreplication_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	doltenginetest "github.com/dolthub/dolt/go/libraries/doltcore/sqle/enginetest"
)

var (
	binlogInsertStmts         = parseBinlogTestFile("binlog_insert.txt")
	binlogUpdateStmts         = parseBinlogTestFile("binlog_update.txt")
	binlogDeleteStmts         = parseBinlogTestFile("binlog_delete.txt")
	binlogFormatDescStmts     = parseBinlogTestFile("binlog_format_desc.txt")
	binlogTransactionMultiOps = parseBinlogTestFile("binlog_transaction_multi_ops.txt")
	binlogNoFormatDescStmts   = parseBinlogTestFile("binlog_no_format_desc.txt")
)

// binlogScripts contains test cases for the BINLOG statement. To add tests: add a @test to binlog_maker.bats, generate
// the .txt file with BINLOG statements, then add a test case here with the corresponding setup.
var binlogScripts = []queries.ScriptTest{
	{
		Name: "SET collation variables with numeric IDs from binlog",
		Assertions: []queries.ScriptTestAssertion{
			// TODO: lc_time_names no-op
			{Query: "SET @@session.lc_time_names=0", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.lc_time_names", Expected: []sql.Row{{"0"}}},
		},
	},
	{
		Name: "BINLOG requires FORMAT_DESCRIPTION_EVENT first",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogNoFormatDescStmts[0], ExpectedErr: sql.ErrNoFormatDescriptionEventBeforeBinlogStatement},
		},
	},
	{
		Name: "BINLOG with simple INSERT",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogInsertStmts[0], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogInsertStmts[1], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogInsertStmts[2], Expected: []sql.Row{{types.OkResult{}}}},
			{
				Query: "SELECT * FROM users ORDER BY id",
				Expected: []sql.Row{
					{1, "Alice", "alice@example.com"},
					{2, "Bob", "bob@example.com"},
				},
			},
		},
	},
	{
		Name: "BINLOG with UPDATE",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogUpdateStmts[0], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogUpdateStmts[1], Expected: []sql.Row{{types.OkResult{}}}},
			{
				Query: "SELECT name FROM users WHERE id = 1",
				Expected: []sql.Row{
					{"Alice Smith"},
				},
			},
		},
	},
	{
		Name: "BINLOG with DELETE",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogDeleteStmts[0], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogDeleteStmts[1], Expected: []sql.Row{{types.OkResult{}}}},
			{
				Query: "SELECT COUNT(*) FROM users",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "SELECT id FROM users",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "BINLOG with FORMAT_DESCRIPTION only",
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogFormatDescStmts[0], Expected: []sql.Row{{types.OkResult{}}}},
		},
	},
	{
		Name: "BINLOG transaction with multiple INSERT UPDATE DELETE",
		SetUpScript: []string{
			"CREATE TABLE multi_op_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), value DECIMAL(10,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{Query: binlogTransactionMultiOps[0], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogTransactionMultiOps[1], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogTransactionMultiOps[2], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogTransactionMultiOps[3], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogTransactionMultiOps[4], Expected: []sql.Row{{types.OkResult{}}}},
			{Query: binlogTransactionMultiOps[5], Expected: []sql.Row{{types.OkResult{}}}},
			{
				Query: "SELECT COUNT(*) FROM multi_op_test",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT value FROM multi_op_test WHERE id = 1",
				Expected: []sql.Row{
					{"109.99"},
				},
			},
		},
	},
	{
		Name: "BINLOG with invalid base64",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "BINLOG 'not-valid-base64!!!'",
				ExpectedErr: sql.ErrBase64DecodeError,
			},
		},
	},
}

// parseBinlogTestFile parses BINLOG statements from a testdata file. The file is pre-filtered by binlog_maker.bats to
// contain only BINLOG statements.
func parseBinlogTestFile(filename string) []string {
	_, sourceFile, _, _ := runtime.Caller(0)
	sourceDir := filepath.Dir(sourceFile)

	testdataPath := filepath.Join(sourceDir, "testdata", filename)

	data, err := os.ReadFile(testdataPath)
	if err != nil {
		return nil
	}

	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil
	}

	parts := strings.Split(content, "BINLOG '")
	var stmts []string
	for i, part := range parts {
		if i == 0 && part == "" {
			continue
		}
		stmts = append(stmts, "BINLOG '"+part)
	}
	return stmts
}

// TestBinlog tests the BINLOG statement functionality using the Dolt engine.
func TestBinlog(t *testing.T) {
	doltenginetest.RunScriptsWithEngineSetup(t, func(engine *gms.Engine) {
		binlogConsumer := binlogreplication.DoltBinlogConsumer
		binlogConsumer.SetEngine(engine)
		engine.EngineAnalyzer().Catalog.BinlogConsumer = binlogConsumer
	}, binlogScripts)
}
