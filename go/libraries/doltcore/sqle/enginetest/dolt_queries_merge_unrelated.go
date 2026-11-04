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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// https://github.com/dolthub/dolt/issues/4468
//
// Merging unrelated histories uses an empty merge base. Branches that fork from the initial commit, whose root is
// empty, already merge against an empty base, so every case runs twice: once with |theirs| forked from the initial
// commit, which documents current behavior, and once with |theirs| as an orphan branch merged with
// --allow-unrelated-histories, which must behave identically.

type emptyBaseMergeCase struct {
	name   string
	ours   []string
	theirs []string
	// mergeResult is the row returned by DOLT_MERGE. Ignored when mergeErr is set.
	mergeResult sql.Row
	mergeErr    string
	assertions  []queries.ScriptTestAssertion
}

var mergeSuccessful = sql.Row{doltCommit, 0, 0, "merge successful"}
var mergeConflicts = sql.Row{"", 0, 1, "conflicts found"}

func sameTableAddedTwiceErr(table string) string {
	return "table with same name '" + table + "' added in 2 commits can't be merged"
}

var emptyBaseMergeCases = []emptyBaseMergeCase{
	// A: table shape
	{
		name:        "disjoint tables",
		ours:        []string{"CREATE TABLE t1 (pk INT PRIMARY KEY, v INT);", "INSERT INTO t1 VALUES (1, 1);"},
		theirs:      []string{"CREATE TABLE t2 (pk INT PRIMARY KEY, v INT);", "INSERT INTO t2 VALUES (2, 2);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SHOW TABLES;", Expected: []sql.Row{{"t1"}, {"t2"}}},
			{Query: "SELECT * FROM t1;", Expected: []sql.Row{{1, 1}}},
			{Query: "SELECT * FROM t2;", Expected: []sql.Row{{2, 2}}},
			{Query: "SELECT COUNT(*) FROM dolt_commit_ancestors WHERE commit_hash = HASHOF('HEAD');", Expected: []sql.Row{{2}}},
		},
	},
	{
		name:     "same table with an extra column",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);"},
		theirs:   []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT, extra INT);"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with a different column type",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);"},
		theirs:   []string{"CREATE TABLE t (pk INT PRIMARY KEY, v VARCHAR(20));"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with a different primary key",
		ours:     []string{"CREATE TABLE t (a INT, b INT, PRIMARY KEY (a));"},
		theirs:   []string{"CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b));"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table keyed on one side and keyless on the other",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);"},
		theirs:   []string{"CREATE TABLE t (pk INT, v INT);"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with columns in a different order",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT);"},
		theirs:   []string{"CREATE TABLE t (pk INT PRIMARY KEY, b INT, a INT);"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with an index on one side",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT, INDEX idx_v (v));"},
		theirs:   []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with a check constraint on one side",
		ours:     []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT, CONSTRAINT chk_v CHECK (v > 0));"},
		theirs:   []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		name:     "same table with a different table collation",
		ours:     []string{"CREATE TABLE t (pk VARCHAR(20) PRIMARY KEY) COLLATE utf8mb4_0900_bin;"},
		theirs:   []string{"CREATE TABLE t (pk VARCHAR(20) PRIMARY KEY) COLLATE utf8mb4_0900_ai_ci;"},
		mergeErr: sameTableAddedTwiceErr("t"),
	},
	{
		// The merge matches columns by name and keeps the tags from ours.
		name: "same visible schema with different column tags",
		ours: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, v INT);",
			"CALL dolt_update_column_tag('t', 'v', 4242);",
			"INSERT INTO t VALUES (1, 1);",
		},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (2, 2);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM t ORDER BY pk;", Expected: []sql.Row{{1, 1}, {2, 2}}},
		},
	},
	{
		name: "same table renamed into place on one side",
		ours: []string{
			"CREATE TABLE u (pk INT PRIMARY KEY, v INT);",
			"RENAME TABLE u TO t;",
			"INSERT INTO t VALUES (1, 1);",
		},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (2, 2);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM t ORDER BY pk;", Expected: []sql.Row{{1, 1}, {2, 2}}},
		},
	},

	// B: rows in a table with the same schema on both sides
	{
		name:        "same table with disjoint rows",
		ours:        []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (1, 1);"},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (2, 2);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM t ORDER BY pk;", Expected: []sql.Row{{1, 1}, {2, 2}}},
		},
	},
	{
		name:        "same table with identical rows",
		ours:        []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (1, 1), (2, 2);"},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (2, 2), (3, 3);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM t ORDER BY pk;", Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}}},
		},
	},
	{
		name:        "same table with conflicting rows",
		ours:        []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (1, 1), (2, 20);"},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "INSERT INTO t VALUES (2, 200), (3, 3);"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"t", uint64(1)}}},
			{
				Query:    "SELECT base_pk, base_v, our_pk, our_v, their_pk, their_v FROM dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 2, 20, 2, 200}},
			},
			{Query: "SELECT * FROM t ORDER BY pk;", Expected: []sql.Row{{1, 1}, {2, 20}, {3, 3}}},
		},
	},
	{
		name:        "same keyless table with overlapping rows",
		ours:        []string{"CREATE TABLE t (v INT);", "INSERT INTO t VALUES (1), (2);"},
		theirs:      []string{"CREATE TABLE t (v INT);", "INSERT INTO t VALUES (2), (3);"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"t", uint64(1)}}},
			{Query: "SELECT v FROM t ORDER BY v;", Expected: []sql.Row{{1}, {2}, {3}}},
		},
	},
	{
		name:        "same keyless table with identical rows",
		ours:        []string{"CREATE TABLE t (v INT);", "INSERT INTO t VALUES (1);"},
		theirs:      []string{"CREATE TABLE t (v INT);", "INSERT INTO t VALUES (1);"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"t", uint64(1)}}},
		},
	},
	{
		name: "unique key violated by rows from both sides",
		ours: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, u INT, UNIQUE KEY uk (u));",
			"INSERT INTO t VALUES (1, 10);",
		},
		theirs: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, u INT, UNIQUE KEY uk (u));",
			"INSERT INTO t VALUES (2, 10);",
		},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT violation_type, pk, u FROM dolt_constraint_violations_t ORDER BY pk;",
				Expected: []sql.Row{{"unique index", 1, 10}, {"unique index", 2, 10}},
			},
		},
	},
	{
		name: "auto increment continues after the largest merged key",
		ours: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY AUTO_INCREMENT, v INT);",
			"INSERT INTO t VALUES (1, 1), (2, 2);",
		},
		theirs: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY AUTO_INCREMENT, v INT);",
			"INSERT INTO t VALUES (10, 10), (11, 11);",
		},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT INTO t (v) VALUES (12);", Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 12}}}},
			{Query: "SELECT pk FROM t WHERE v = 12;", Expected: []sql.Row{{12}}},
		},
	},
	{
		name: "full-text index on the same table",
		ours: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, body VARCHAR(100), FULLTEXT idx_body (body));",
			"INSERT INTO t VALUES (1, 'ours apple');",
		},
		theirs: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, body VARCHAR(100), FULLTEXT idx_body (body));",
			"INSERT INTO t VALUES (2, 'theirs apple');",
		},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT pk FROM t WHERE MATCH(body) AGAINST('apple') ORDER BY pk;", Expected: []sql.Row{{1}, {2}}},
		},
	},

	// C: constraints that span tables
	{
		name: "foreign key on one side violated by child rows from the other",
		ours: []string{
			"CREATE TABLE parent (id INT PRIMARY KEY);",
			"CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES parent (id));",
			"INSERT INTO parent VALUES (1);",
			"INSERT INTO child VALUES (1, 1);",
		},
		theirs: []string{
			"CREATE TABLE parent (id INT PRIMARY KEY);",
			"CREATE TABLE child (id INT PRIMARY KEY, pid INT, INDEX fk_pid (pid));",
			"INSERT INTO child VALUES (2, 99);",
		},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT violation_type, id, pid FROM dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 2, 99}},
			},
		},
	},
	{
		name: "identical foreign keys on both sides with disjoint rows",
		ours: []string{
			"CREATE TABLE parent (id INT PRIMARY KEY);",
			"CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES parent (id));",
			"INSERT INTO parent VALUES (1);",
			"INSERT INTO child VALUES (1, 1);",
		},
		theirs: []string{
			"CREATE TABLE parent (id INT PRIMARY KEY);",
			"CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES parent (id));",
			"INSERT INTO parent VALUES (2);",
			"INSERT INTO child VALUES (2, 2);",
		},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM child ORDER BY id;", Expected: []sql.Row{{1, 1}, {2, 2}}},
			{Query: "SELECT COUNT(*) FROM information_schema.referential_constraints;", Expected: []sql.Row{{1}}},
		},
	},

	// D: schema fragments, docs, and ignore rules
	{
		name:        "views with different names",
		ours:        []string{"CREATE VIEW v1 AS SELECT 1 AS x;"},
		theirs:      []string{"CREATE VIEW v2 AS SELECT 2 AS x;"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM v1;", Expected: []sql.Row{{1}}},
			{Query: "SELECT * FROM v2;", Expected: []sql.Row{{2}}},
		},
	},
	{
		name:        "views with the same name and different definitions",
		ours:        []string{"CREATE VIEW v AS SELECT 1 AS x;"},
		theirs:      []string{"CREATE VIEW v AS SELECT 2 AS x;"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"dolt_schemas", uint64(1)}}},
		},
	},
	{
		name:        "triggers with the same name and different definitions",
		ours:        []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET new.v = 1;"},
		theirs:      []string{"CREATE TABLE t (pk INT PRIMARY KEY, v INT);", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET new.v = 2;"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"dolt_schemas", uint64(1)}}},
		},
	},
	{
		name:        "docs with the same name and different contents",
		ours:        []string{"INSERT INTO dolt_docs VALUES ('README.md', 'ours');"},
		theirs:      []string{"INSERT INTO dolt_docs VALUES ('README.md', 'theirs');"},
		mergeResult: mergeConflicts,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT * FROM dolt_conflicts;", Expected: []sql.Row{{"dolt_docs", uint64(1)}}},
		},
	},
	{
		name:        "ignore patterns from both sides",
		ours:        []string{"INSERT INTO dolt_ignore VALUES ('ours_*', true);"},
		theirs:      []string{"INSERT INTO dolt_ignore VALUES ('theirs_*', true);"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT pattern FROM dolt_ignore ORDER BY pattern;", Expected: []sql.Row{{"ours_*"}, {"theirs_*"}}},
		},
	},
	{
		name:        "stored procedures with different names",
		ours:        []string{"CREATE PROCEDURE p1() SELECT 1;"},
		theirs:      []string{"CREATE PROCEDURE p2() SELECT 2;"},
		mergeResult: mergeSuccessful,
		assertions: []queries.ScriptTestAssertion{
			{Query: "SELECT name FROM dolt_procedures ORDER BY name;", Expected: []sql.Row{{"p1"}, {"p2"}}},
		},
	},
}

// emptyBaseMergeScript builds the two histories for |c|. With |unrelated|, theirs is an orphan branch and the merge
// passes --allow-unrelated-histories; otherwise theirs forks from the initial commit.
func emptyBaseMergeScript(c emptyBaseMergeCase, unrelated bool) queries.ScriptTest {
	createTheirs := "CALL DOLT_BRANCH('theirs');"
	mergeQuery := "CALL DOLT_MERGE('theirs');"
	name := "shared empty base: " + c.name
	if unrelated {
		createTheirs = "CALL DOLT_BRANCH('--orphan', 'theirs');"
		mergeQuery = "CALL DOLT_MERGE('--allow-unrelated-histories', 'theirs');"
		name = "unrelated histories: " + c.name
	}

	setup := []string{"SET dolt_allow_commit_conflicts = on;", "SET dolt_force_transaction_commit = on;", createTheirs}
	setup = append(setup, c.ours...)
	setup = append(setup, "CALL DOLT_COMMIT('-Am', 'ours');", "CALL DOLT_CHECKOUT('theirs');")
	setup = append(setup, c.theirs...)
	setup = append(setup, "CALL DOLT_COMMIT('-Am', 'theirs');", "CALL DOLT_CHECKOUT('main');")

	merge := queries.ScriptTestAssertion{Query: mergeQuery}
	if c.mergeErr != "" {
		merge.ExpectedErrStr = c.mergeErr
	} else {
		merge.Expected = []sql.Row{c.mergeResult}
	}

	return queries.ScriptTest{
		Name:        name,
		SetUpScript: setup,
		Assertions:  append([]queries.ScriptTestAssertion{merge}, c.assertions...),
	}
}

// SharedEmptyBaseMergeScripts merge two branches whose only common ancestor is the initial commit.
func SharedEmptyBaseMergeScripts() []queries.ScriptTest {
	var scripts []queries.ScriptTest
	for _, c := range emptyBaseMergeCases {
		scripts = append(scripts, emptyBaseMergeScript(c, false))
	}
	return scripts
}

// UnrelatedHistoriesMergeScripts merge two branches that have no common ancestor.
func UnrelatedHistoriesMergeScripts() []queries.ScriptTest {
	scripts := []queries.ScriptTest{
		{
			Name: "unrelated histories: merge without --allow-unrelated-histories is refused",
			SetUpScript: []string{
				"CALL DOLT_BRANCH('--orphan', 'theirs');",
				"CREATE TABLE t1 (pk INT PRIMARY KEY);",
				"CALL DOLT_COMMIT('-Am', 'ours');",
			},
			Assertions: []queries.ScriptTestAssertion{
				{Query: "CALL DOLT_MERGE('theirs');", ExpectedErrStr: "refusing to merge unrelated histories"},
				{Query: "SHOW TABLES;", Expected: []sql.Row{{"t1"}}},
			},
		},
	}
	for _, c := range emptyBaseMergeCases {
		scripts = append(scripts, emptyBaseMergeScript(c, true))
	}
	return scripts
}
