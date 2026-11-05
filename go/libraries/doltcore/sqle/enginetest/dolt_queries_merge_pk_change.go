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
)

// samePkChangeSetup creates |createTable| and |inserts| on main, branches feature, and then runs |alter| on both
// branches, followed by |mainEdits| on main and |featureEdits| on feature. It ends on main.
func samePkChangeSetup(createTable string, inserts []string, alter string, mainEdits, featureEdits []string) []string {
	return pkChangeHistory(append([]string{createTable}, inserts...),
		append([]string{alter}, mainEdits...),
		append([]string{alter}, featureEdits...))
}

// pkChangeHistory runs |base| on main and commits it, branches feature, and then commits |mainSteps| on main and
// |featureSteps| on feature. It ends on main.
func pkChangeHistory(base, mainSteps, featureSteps []string) []string {
	s := append([]string{}, base...)
	s = append(s, "CALL DOLT_COMMIT('-Am', 'base');", "CALL DOLT_BRANCH('feature');")
	s = append(s, mainSteps...)
	s = append(s, "CALL DOLT_COMMIT('-Am', 'main');", "CALL DOLT_CHECKOUT('feature');")
	s = append(s, featureSteps...)
	return append(s, "CALL DOLT_COMMIT('-Am', 'feature');", "CALL DOLT_CHECKOUT('main');")
}

const pkChangeTable = "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a), KEY vidx (v));"
const pkChangeAlter = "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);"

var pkChangeRows = []string{"INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30);"}

// MergeSamePrimaryKeyChangeScripts merge branches that made the same primary key change independently, so that their
// common ancestor is keyed differently from both of them. The merge rekeys the ancestor to their primary key.
var MergeSamePrimaryKeyChangeScripts = []queries.ScriptTest{
	{
		Name: "same pk change on both sides: merge",
		SetUpScript: samePkChangeSetup(pkChangeTable, pkChangeRows, pkChangeAlter,
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 11}, {2, 2, 20}, {3, 3, 30}},
			},
			{
				Query:    "SELECT a, b FROM t WHERE v = 11;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query: "SHOW CREATE TABLE t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int NOT NULL,\n" +
					"  `v` int,\n" +
					"  PRIMARY KEY (`a`,`b`),\n" +
					"  KEY `vidx` (`v`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "same pk change on both sides: merge in the other direction",
		SetUpScript: samePkChangeSetup(pkChangeTable, pkChangeRows, pkChangeAlter,
			[]string{"DELETE FROM t WHERE a = 3;", "INSERT INTO t VALUES (4, 4, 40);"},
			[]string{"UPDATE t SET v = 11 WHERE a = 1;", "UPDATE t SET v = 22 WHERE a = 2;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_CHECKOUT('feature');",
				Expected: []sql.Row{{0, "Switched to branch 'feature'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 11}, {2, 2, 22}, {4, 4, 40}},
			},
			{
				Query:    "SELECT a FROM t WHERE v = 40;",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "same pk change on both sides: squash merge",
		SetUpScript: samePkChangeSetup(pkChangeTable, pkChangeRows, pkChangeAlter,
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '--squash');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 11}, {2, 2, 20}, {3, 3, 30}},
			},
		},
	},
	{
		Name: "same pk change on both sides: key columns reordered",
		SetUpScript: samePkChangeSetup(
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b));",
			pkChangeRows,
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b, a);",
			[]string{"UPDATE t SET v = 21 WHERE a = 2;"},
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 11}, {2, 2, 21}, {3, 3, 30}},
			},
		},
	},
	{
		Name: "same pk change on both sides: string key column",
		SetUpScript: samePkChangeSetup(
			"CREATE TABLE t (id INT NOT NULL, name VARCHAR(20) NOT NULL, v INT, PRIMARY KEY (id));",
			[]string{"INSERT INTO t VALUES (1, 'one', 10), (2, 'two', 20);"},
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (name, id);",
			[]string{"INSERT INTO t VALUES (3, 'three', 30);"},
			[]string{"UPDATE t SET v = 11 WHERE id = 1;", "DELETE FROM t WHERE id = 2;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "one", 11}, {3, "three", 30}},
			},
		},
	},
	{
		Name: "same pk change on both sides: conflicts show the rekeyed base",
		SetUpScript: append(samePkChangeSetup(pkChangeTable, pkChangeRows, pkChangeAlter,
			[]string{"UPDATE t SET v = 100 WHERE a = 1;", "DELETE FROM t WHERE a = 2;"},
			[]string{"UPDATE t SET v = 11 WHERE a = 1;", "UPDATE t SET v = 21 WHERE a = 2;"}),
			"SET @@autocommit = 0;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT `table`, num_data_conflicts FROM dolt_preview_merge_conflicts_summary('main', 'feature');",
				Expected: []sql.Row{{"t", uint64(2)}},
			},
			{
				Query: "SELECT base_a, base_b, base_v, our_v, their_v " +
					"FROM dolt_preview_merge_conflicts('main', 'feature', 't') ORDER BY base_a;",
				Expected: []sql.Row{{1, 1, 10, 100, 11}, {2, 2, 20, nil, 21}},
			},
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "SELECT base_a, base_b, base_v, our_a, our_b, our_v, our_diff_type, their_v, their_diff_type " +
					"FROM dolt_conflicts_t ORDER BY base_a;",
				Expected: []sql.Row{
					{1, 1, 10, 1, 1, 100, "modified", 11, "modified"},
					{2, 2, 20, nil, nil, nil, "removed", 21, "modified"},
				},
			},
			{
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 11}, {2, 2, 21}, {3, 3, 30}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_conflicts;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "same pk change on both sides: unique index violation",
		SetUpScript: append(samePkChangeSetup(
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a), UNIQUE KEY uv (v));",
			pkChangeRows, pkChangeAlter,
			[]string{"INSERT INTO t VALUES (4, 4, 40);"},
			[]string{"INSERT INTO t VALUES (5, 5, 40);"}),
			"SET @@autocommit = 0;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, a, b, v FROM dolt_constraint_violations_t ORDER BY a;",
				Expected: []sql.Row{{"unique index", 4, 4, 40}, {"unique index", 5, 5, 40}},
			},
		},
	},
	{
		Name: "same pk change on both sides: foreign key violation",
		SetUpScript: []string{
			"CREATE TABLE p (id INT PRIMARY KEY, k INT NOT NULL, UNIQUE KEY (k));",
			"CREATE TABLE c (a INT NOT NULL, b INT NOT NULL, pk INT, PRIMARY KEY (a), FOREIGN KEY (pk) REFERENCES p (k));",
			"INSERT INTO p VALUES (1, 100), (2, 200);",
			"INSERT INTO c VALUES (1, 1, 100), (2, 2, 200);",
			"CALL DOLT_COMMIT('-Am', 'base');",
			"CALL DOLT_BRANCH('feature');",
			"ALTER TABLE c DROP PRIMARY KEY, ADD PRIMARY KEY (b, a);",
			"DELETE FROM c WHERE a = 2;",
			"DELETE FROM p WHERE id = 2;",
			"CALL DOLT_COMMIT('-am', 'main: change pk, delete parent 200');",
			"CALL DOLT_CHECKOUT('feature');",
			"ALTER TABLE c DROP PRIMARY KEY, ADD PRIMARY KEY (b, a);",
			"INSERT INTO c VALUES (3, 3, 200);",
			"CALL DOLT_COMMIT('-am', 'feature: change pk, reference parent 200');",
			"CALL DOLT_CHECKOUT('main');",
			"SET @@autocommit = 0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * FROM c ORDER BY a;",
				Expected: []sql.Row{{1, 1, 100}, {3, 3, 200}},
			},
			{
				Query:    "SELECT violation_type, a, b, pk FROM dolt_constraint_violations_c;",
				Expected: []sql.Row{{"foreign key", 3, 3, 200}},
			},
		},
	},
	{
		// Both sides ran the same migration on t: remove the row whose b duplicates another, then key t on b. The
		// branches differ only in another table, so t is the same on both sides and the merge takes it without merging
		// it. The ancestor of t has two rows with the same b and cannot be rekeyed, but the merge never reads it, so
		// branches that ran the same migration must not fail to merge because of it.
		Name: "same pk change on both sides: identical sides merge although ancestor rows collide under the new key",
		SetUpScript: []string{
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a));",
			"INSERT INTO t VALUES (1, 7, 10), (2, 7, 20);",
			"CREATE TABLE other (pk INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'base');",
			"CALL DOLT_BRANCH('feature');",

			"DELETE FROM t WHERE a = 2;",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"CALL DOLT_COMMIT('-Am', 'main');",

			"CALL DOLT_CHECKOUT('feature');",
			"DELETE FROM t WHERE a = 2;",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"INSERT INTO other VALUES (1);",
			"CALL DOLT_COMMIT('-Am', 'feature');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, 7, 10}},
			},
			{
				Query:    "SELECT * FROM other;",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		// Both sides ran the same migration on t: fill in the NULL in b, then key t on (a, b). As above, t is the
		// same on both sides, so its ancestor, which has a NULL in the new primary key, must not fail the merge.
		Name: "same pk change on both sides: identical sides merge although the ancestor has a NULL in the new key",
		SetUpScript: []string{
			"CREATE TABLE t (a INT NOT NULL, b INT, v INT, PRIMARY KEY (a));",
			"INSERT INTO t VALUES (1, NULL, 10), (2, 2, 20);",
			"CREATE TABLE other (pk INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'base');",
			"CALL DOLT_BRANCH('feature');",

			"UPDATE t SET b = 1 WHERE a = 1;",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-Am', 'main');",

			"CALL DOLT_CHECKOUT('feature');",
			"UPDATE t SET b = 1 WHERE a = 1;",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);",
			"INSERT INTO other VALUES (1);",
			"CALL DOLT_COMMIT('-Am', 'feature');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1, 10}, {2, 2, 20}},
			},
		},
	},

	{
		// Both sides ran the same migration on c, a foreign key child whose ancestor can be rekeyed. The foreign key
		// checks diff c against its rekeyed ancestor, so the row whose parent was already missing is not reported as a
		// new violation.
		Name: "same pk change on both sides: identical foreign key child keeps its existing dangling reference",
		SetUpScript: []string{
			"CREATE TABLE p (id INT PRIMARY KEY);",
			"INSERT INTO p VALUES (1);",
			"CREATE TABLE c (a INT NOT NULL, b INT NOT NULL, pid INT, PRIMARY KEY (a), FOREIGN KEY (pid) REFERENCES p (id));",
			"SET foreign_key_checks = 0;",
			"INSERT INTO c VALUES (1, 10, 1), (2, 20, 99);",
			"SET foreign_key_checks = 1;",
			"CREATE TABLE other (pk INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'base');",
			"CALL DOLT_BRANCH('feature');",

			"ALTER TABLE c DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"CALL DOLT_COMMIT('-Am', 'main');",

			"CALL DOLT_CHECKOUT('feature');",
			"ALTER TABLE c DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"INSERT INTO other VALUES (1);",
			"CALL DOLT_COMMIT('-Am', 'feature');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM dolt_constraint_violations_c;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// x and y both changed the primary key of t from (a) to (b), and changed row 1 differently. m1 and m2 each
		// merge x and y, so merging m2 into m1 has the two merge bases x and y, and builds a virtual merge base by
		// merging them against t's ancestor rekeyed to (b). The virtual merge base must read each conflicted row's
		// base from that rekeyed ancestor. The row with b = 2 is a = 1, while the ancestor keyed on a holds a = 2 there.
		Name: "same pk change on both sides: criss-cross merge reads the rekeyed ancestor",
		SetUpScript: []string{
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, w INT, PRIMARY KEY (a));",
			"INSERT INTO t VALUES (1, 2, 10, 100), (2, 1, 20, 200);",
			"CALL DOLT_COMMIT('-Am', 'base');",
			"CALL DOLT_BRANCH('x');",
			"CALL DOLT_BRANCH('y');",

			"CALL DOLT_CHECKOUT('x');",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"UPDATE t SET v = 11, w = 150 WHERE a = 1;",
			"CALL DOLT_COMMIT('-am', 'x');",

			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);",
			"UPDATE t SET v = 12 WHERE a = 1;",
			"CALL DOLT_COMMIT('-am', 'y');",

			"SET @@autocommit = 0;",
			"SET @@dolt_allow_commit_conflicts = 1;",
			"CALL DOLT_CHECKOUT('-b', 'm1', 'x');",
			"CALL DOLT_MERGE('y');",
			"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
			"CALL DOLT_COMMIT('-Am', 'm1');",

			"CALL DOLT_CHECKOUT('-b', 'm2', 'y');",
			"CALL DOLT_MERGE('x');",
			"CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
			"UPDATE t SET w = 160 WHERE a = 1;",
			"CALL DOLT_COMMIT('-Am', 'm2');",
			"CALL DOLT_CHECKOUT('m1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_merge_bases('m1', 'm2');",
				Expected: []sql.Row{{2}},
			},
			{
				// Both sides hold v = 11, and only m2 changed w since the virtual merge base, so the row merges.
				Query:    "CALL DOLT_MERGE('m2');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 2, 11, 160}, {2, 1, 20, 200}},
			},
		},
	},
	{
		// Each branch changed t's key and merged the data-free schema branch that changed it too, so main and feature
		// have two merge bases: the fork point, keyed the old way, and schema, keyed the new way. Building the virtual
		// merge base rekeys the fork point to the new key rather than failing on a one-sided key change.
		Name: "same pk change on both sides: merge bases keyed differently",
		SetUpScript: []string{
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, d INT NOT NULL, PRIMARY KEY (a, b, d));",
			"CALL DOLT_COMMIT('-Am', 'create t');",
			"CALL DOLT_BRANCH('schema');",
			"INSERT INTO t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'row on main');",
			"CALL DOLT_BRANCH('feature');",

			"CALL DOLT_CHECKOUT('schema');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on schema');",

			"CALL DOLT_CHECKOUT('main');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on main');",
			"CALL DOLT_MERGE('schema', '-m', 'merge schema into main');",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'another row on main');",

			"CALL DOLT_CHECKOUT('feature');",
			"INSERT INTO t VALUES (3, 3, 1);",
			"CALL DOLT_COMMIT('-am', 'row on feature');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on feature');",
			"CALL DOLT_MERGE('schema', '-m', 'merge schema into feature');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_merge_bases('main', 'feature');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		// As above, with two schema branches, so main and feature have three merge bases. Folding the third into the
		// virtual merge base of the first two also rekeys the fork point.
		Name: "same pk change on both sides: three merge bases keyed differently",
		SetUpScript: []string{
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, d INT NOT NULL, PRIMARY KEY (a, b, d));",
			"CALL DOLT_COMMIT('-Am', 'create t');",
			"CALL DOLT_BRANCH('schema');",
			"CALL DOLT_BRANCH('schema2');",
			"INSERT INTO t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'row on main');",
			"CALL DOLT_BRANCH('feature');",

			"CALL DOLT_CHECKOUT('schema');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on schema');",

			"CALL DOLT_CHECKOUT('schema2');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CREATE TABLE u (pk INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'key change on schema2');",

			"CALL DOLT_CHECKOUT('main');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on main');",
			"CALL DOLT_MERGE('schema', '-m', 'merge schema into main');",
			"CALL DOLT_MERGE('schema2', '-m', 'merge schema2 into main');",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'another row on main');",

			"CALL DOLT_CHECKOUT('feature');",
			"ALTER TABLE t DROP PRIMARY KEY, DROP COLUMN d, ADD PRIMARY KEY (a, b);",
			"CALL DOLT_COMMIT('-am', 'key change on feature');",
			"CALL DOLT_MERGE('schema', '-m', 'merge schema into feature');",
			"CALL DOLT_MERGE('schema2', '-m', 'merge schema2 into feature');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'row on feature');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_merge_bases('main', 'feature');",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY a;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	// The merge refuses the cases below with an error rather than merging them.
	{
		Name: "same pk change on both sides: ancestor rows collide under the new key",
		SetUpScript: pkChangeHistory(
			[]string{
				"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a));",
				"INSERT INTO t VALUES (1, 7, 10), (2, 7, 20);",
			},
			[]string{"UPDATE t SET b = 8 WHERE a = 2;", "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);"},
			[]string{"DELETE FROM t WHERE a = 2;", "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b);"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge table t: both sides changed its primary key, and two rows of the common ancestor have the same new primary key",
			},
		},
	},
	{
		Name: "same pk change on both sides: ancestor has a NULL in the new key",
		SetUpScript: pkChangeHistory(
			[]string{
				"CREATE TABLE t (a INT NOT NULL, b INT, v INT, PRIMARY KEY (a));",
				"INSERT INTO t VALUES (1, NULL, 10), (2, 2, 20);",
			},
			[]string{"UPDATE t SET b = 1 WHERE a = 1;", pkChangeAlter},
			[]string{"UPDATE t SET b = 1 WHERE a = 1;", pkChangeAlter, "UPDATE t SET v = 21 WHERE a = 2;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge table t: both sides changed its primary key, and a row of the common ancestor has a NULL in the new primary key",
			},
		},
	},
	{
		Name: "pk changed differently on each side",
		SetUpScript: pkChangeHistory(
			append([]string{pkChangeTable}, pkChangeRows...),
			[]string{pkChangeAlter},
			[]string{"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (b, a);"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys",
			},
		},
	},
	{
		Name: "pk changed on one side only",
		SetUpScript: pkChangeHistory(
			append([]string{pkChangeTable}, pkChangeRows...),
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"},
			[]string{pkChangeAlter}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys",
			},
		},
	},
	{
		// TODO: a keyless ancestor could be rekeyed too, when none of its rows has more than one copy.
		Name: "same pk added on both sides to a keyless table",
		SetUpScript: samePkChangeSetup(
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT);",
			pkChangeRows,
			"ALTER TABLE t ADD PRIMARY KEY (a);",
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys in its common ancestor",
			},
		},
	},
	{
		Name: "same pk change on both sides: new key column is not in the ancestor",
		SetUpScript: samePkChangeSetup(pkChangeTable, pkChangeRows,
			"ALTER TABLE t ADD COLUMN c INT NOT NULL DEFAULT 0, DROP PRIMARY KEY, ADD PRIMARY KEY (a, c);",
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys in its common ancestor",
			},
		},
	},
	{
		Name: "same pk change on both sides: new key column changed type",
		SetUpScript: samePkChangeSetup(pkChangeTable, pkChangeRows,
			"ALTER TABLE t MODIFY COLUMN b BIGINT NOT NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);",
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys in its common ancestor",
			},
		},
	},
	{
		Name: "same pk change on both sides: ancestor has a virtual column",
		SetUpScript: samePkChangeSetup(
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, g INT AS (a + b) VIRTUAL, PRIMARY KEY (a));",
			[]string{"INSERT INTO t (a, b, v) VALUES (1, 1, 10), (2, 2, 20);"},
			pkChangeAlter,
			nil,
			[]string{"UPDATE t SET v = 11 WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys in its common ancestor",
			},
		},
	},
	{
		Name: "same pk change on both sides: ancestor has a Full-Text index",
		SetUpScript: samePkChangeSetup(
			"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, s VARCHAR(100), PRIMARY KEY (a), FULLTEXT INDEX fs (s));",
			[]string{"INSERT INTO t VALUES (1, 1, 'one'), (2, 2, 'two');"},
			pkChangeAlter,
			nil,
			[]string{"UPDATE t SET s = 'uno' WHERE a = 1;"}),
		Assertions: []queries.ScriptTestAssertion{
			{
				// TODO: The Full-Text pseudo-index tables are keyed by t's primary key too, and the error names one of
				// them rather than t.
				Query:          "CALL DOLT_MERGE('feature');",
				ExpectedErrStr: "error: cannot merge because table dolt_t_fs_0_fts_doc_count has different primary keys in its common ancestor",
			},
		},
	},
}
