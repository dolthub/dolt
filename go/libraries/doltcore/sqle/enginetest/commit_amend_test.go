// Copyright 2022 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestCommitMessageAmend(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestScript(t, harness, queries.ScriptTest{
		Name: "Sysbench Transactions Shouldn't Cause Constraint Violations",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",
			"CREATE TABLE test (id INT PRIMARY KEY );",
			"INSERT INTO test (id) VALUES (1)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'modified commit message');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"modified commit message"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	})
}
func TestCommitChangesAmend(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestScript(t, harness, queries.ScriptTest{
		Name: "Sysbench Transactions Shouldn't Cause Constraint Violations",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",
			"CREATE TABLE test (id INT PRIMARY KEY );",
			"INSERT INTO test (id) VALUES (1)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:            "INSERT INTO test (id) VALUES (2)",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:            "CALL DOLT_ADD('.');",
				SkipResultsCheck: true,
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:            "INSERT INTO test (id) VALUES (3)",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:            "CALL DOLT_ADD('.');",
				SkipResultsCheck: true,
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'updated commit message');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"updated commit message"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	})
}
func TestMergeAmend(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestScript(t, harness, queries.ScriptTest{
		Name: "Sysbench Transactions Shouldn't Cause Constraint Violations",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",

			"CREATE TABLE test (id INT PRIMARY KEY, id2 INT);",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original table');",

			"CALL DOLT_CHECKOUT('-b','test-branch');",
			"INSERT INTO test (id, id2) VALUES (0, 2)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'conflicting commit message');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test (id, id2) VALUES (0, 1)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",

			"CALL DOLT_MERGE('test-branch');",
			"CALL DOLT_CONFLICTS_RESOLVE('--theirs', '.');",
			"CALL DOLT_COMMIT('-m', 'final merge');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'new merge');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"new merge"},
					{"original commit message"},
					{"conflicting commit message"},
					{"original table"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"}},
			},
			{
				Query:            "SET @hash=(SELECT commit_hash FROM dolt_log LIMIT 1);",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT COUNT(parent_hash) FROM dolt_commit_ancestors WHERE commit_hash= @hash;",
				Expected: []sql.Row{{2}},
			},
		},
	})
}
