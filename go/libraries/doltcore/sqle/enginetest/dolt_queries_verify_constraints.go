// Copyright 2024 Dolthub, Inc.
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

var verifyConstraintsUniqueViolationsSetupScript = []string{
	"create table otherTable (pk int primary key);",
	"create table t (pk int primary key, col1 int, unique key (col1));",
	"call dolt_commit('-Am', 'initial commit');",
	"call dolt_branch('branch1');",
	"insert into t (pk, col1) values (1, 1);",
	"call dolt_commit('-am', 'insert on main');",
	"call dolt_checkout('branch1');",
	"insert into t (pk, col1) values (2, 1);",
	"call dolt_commit('-am', 'insert on branch1');",
	"set @@autocommit=0;",
}

var verifyConstraintsCheckViolationsSetupScript = []string{
	"create table otherTable (pk int primary key);",
	"create table t (pk int primary key, col1 int, col2 int, check (col1 != col2));",
	"insert into t values (1, 2, 3);",
	"call dolt_commit('-Am', 'initial commit');",
	"call dolt_branch('branch1');",
	"update t set col1 = 42;",
	"call dolt_commit('-am', 'update on main');",
	"call dolt_checkout('branch1');",
	"update t set col2 = 42;",
	"call dolt_commit('-am', 'update on branch1');",
	"set @@autocommit=0;",
}

var verifyConstraintsFkViolationsSetupScript = []string{
	"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
	"CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name1 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
	"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
	"CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name2 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
	"CALL DOLT_ADD('.')",
	"INSERT INTO parent3 VALUES (1, 1);",
	"INSERT INTO parent4 VALUES (2, 2);",
	"SET foreign_key_checks=0;",
	"INSERT INTO child3 VALUES (1, 1), (2, 2);",
	"INSERT INTO child4 VALUES (1, 1), (2, 2);",
	"SET foreign_key_checks=1;",
	"CALL DOLT_COMMIT('-afm', 'has fk violations');",
	`
	CREATE TABLE parent1 (
  		pk BIGINT PRIMARY KEY,
  		v1 BIGINT,
  		INDEX (v1)
	);`,
	`
	CREATE TABLE parent2 (
	  pk BIGINT PRIMARY KEY,
	  v1 BIGINT,
	  INDEX (v1)
	);`,
	`
	CREATE TABLE child1 (
	  pk BIGINT PRIMARY KEY,
	  parent1_v1 BIGINT,
	  parent2_v1 BIGINT,
	  CONSTRAINT child1_parent1 FOREIGN KEY (parent1_v1) REFERENCES parent1 (v1),
	  CONSTRAINT child1_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
	);`,
	`
	CREATE TABLE child2 (
	  pk BIGINT PRIMARY KEY,
	  parent2_v1 BIGINT,
	  CONSTRAINT child2_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
	);`,
	"INSERT INTO parent1 VALUES (1,1), (2,2), (3,3);",
	"INSERT INTO parent2 VALUES (1,1), (2,2), (3,3);",
	"INSERT INTO child1 VALUES (1,1,1), (2,2,2);",
	"INSERT INTO child2 VALUES (2,2), (3,3);",
	"SET foreign_key_checks=0;",
	"INSERT INTO child3 VALUES (3, 3);",
	"INSERT INTO child4 VALUES (3, 3);",
	"SET foreign_key_checks=1;",
}

var DoltVerifyConstraintsTestScripts = []queries.ScriptTest{
	// Foreign Key Constraint Violations
	{
		Name:        "verify-constraints: no FK violations",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child1')",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child1');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: no named tables",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS();",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: named table",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child3');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: named tables",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child3', 'child4');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: --all no named tables",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: --all named table",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child3');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: --all named tables",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child3', 'child4');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: --output-only",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--output-only', 'child3', 'child4');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name:        "verify-constraints: FK violations: --all --output-only",
		SetUpScript: verifyConstraintsFkViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', '--output-only', 'child3', 'child4');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "verify-constraints: FK violations: bad compound primary key reuse as index - no error",
		SetUpScript: []string{
			"create table parent (col1 int not null, col2 float not null, primary key (col1, col2));",
			"create table child (col1 int not null, col2 float not null, col3 int not null, col4 float not null, col5 int not null, col6 float not null, primary key (col1, col2, col3, col4, col5, col6), foreign key (col1, col2) references parent (col1, col2));",
			"set foreign_key_checks = 0;",
			"insert into parent values (1, 2.5), (7, 8.5);",
			"insert into child values (1, 2.5, 3, 4.5, 5, 6.5), (7, 8.5, 9, 10.5, 11, 12.5);",
			"set foreign_key_checks = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "verify-constraints: FK violations: bad compound primary key reuse as index - error",
		SetUpScript: []string{
			"create table parent (col1 int not null, col2 float not null, primary key (col1, col2));",
			"create table child (col1 int not null, col2 float not null, col3 int not null, col4 float not null, col5 int not null, col6 float not null, primary key (col1, col2, col3, col4, col5, col6), foreign key (col1, col2) references parent (col1, col2));",
			"set foreign_key_checks = 0;",
			"insert into parent values (1, 2.5);",
			"insert into child values (1, 2.5, 3, 4.5, 5, 6.5), (7, 8.5, 9, 10.5, 11, 12.5);",
			"set foreign_key_checks = 1;",
			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"child", uint64(1)}},
			},
		},
	},
	{
		Name: "verify-constraints: FK violations: ignores null",
		SetUpScript: []string{
			"create table parent (id bigint primary key, v1 bigint, v2 bigint, index (v1, v2))",
			"create table child (id bigint primary key, v1 bigint, v2 bigint, foreign key (v1, v2) references parent(v1, v2))",
			"insert into parent values (1, 1, 1), (2, 2, 2)",
			"insert into child values (1, 1, 1), (2, 90, NULL)",
			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child')",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:            "set foreign_key_checks = 0;",
				SkipResultsCheck: true,
			},
			{
				Query:    "insert into child values (3, 30, 30);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:            "set foreign_key_checks = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child')",
				Expected: []sql.UntypedSqlRow{{1}},
			},
		},
	},

	// Unique Constraint Violations
	{
		Name:        "verify-constraints: unique violations: working set",
		SetUpScript: verifyConstraintsUniqueViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: unique violations: --all",
		SetUpScript: verifyConstraintsUniqueViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "call dolt_commit('-am', 'commiting with conflicts', '--force');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				// no violations in the working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// one unique violation in all the data
				Query:    "call dolt_verify_constraints('--all');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: unique violations: working set with named table",
		SetUpScript: verifyConstraintsUniqueViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints('t');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: unique violations: working set with named table with no violation",
		SetUpScript: verifyConstraintsUniqueViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints('otherTable');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// Nothing in dolt_constraint_violations because we only verified otherTable
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name:        "verify-constraints: unique violations: --all --output-only",
		SetUpScript: verifyConstraintsUniqueViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "call dolt_commit('-am', 'commiting with conflicts', '--force');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(2)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"unique index", 1, 1, `{"Name": "col1", "Columns": ["col1"]}`},
					{"unique index", 2, 1, `{"Name": "col1", "Columns": ["col1"]}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				// no violations in the working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// one unique violation in all the data
				Query:    "call dolt_verify_constraints('--all', '--output-only');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				// no output recorded because of --output-only
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},

	// Check Constraint Violations
	{
		Name:        "verify-constraints: check violations: working set",
		SetUpScript: verifyConstraintsCheckViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 42, 42}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: check violations: --all",
		SetUpScript: verifyConstraintsCheckViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "call dolt_commit('-am', 'commiting with conflicts', '--force');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 42, 42}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// no violations in the working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// one unique violation in all the data
				Query:    "call dolt_verify_constraints('--all');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: check violations: working set with named table",
		SetUpScript: verifyConstraintsCheckViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 42, 42}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints('t');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
		},
	},
	{
		Name:        "verify-constraints: check violations: working set with named table with no violation",
		SetUpScript: verifyConstraintsCheckViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				// merge with --squash so that our working set has the constraint violations
				Query:    "call dolt_merge('main', '--squash');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 42, 42}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// verify constraints in working set
				Query:    "call dolt_verify_constraints('otherTable');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// Nothing in dolt_constraint_violations because we only verify otherTable
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name:        "verify-constraints: check violations: --all --output-only",
		SetUpScript: verifyConstraintsCheckViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "call dolt_commit('-am', 'commiting with conflicts', '--force');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, 42, 42}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				Query: "select violation_type, pk, col1, cast(violation_info as char) as violation_info from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{
					{"check constraint", 1, 42, `{"Name": "t_chk_5eebhnk4", "Expression": "(NOT((col1 = col2)))"}`},
				},
			},
			{
				Query:    "delete from dolt_constraint_violations_t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				// no violations in the working set
				Query:    "call dolt_verify_constraints();",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// one unique violation in all the data
				Query:    "call dolt_verify_constraints('--all', '--output-only');",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				// no output recorded because of --output-only
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	// NOTE: We can't check NOT NULL constraint violations, since there isn't a way to disable NOT NULL enforcement
	//       (like there is for foreign keys), and merging in a NOT NULL schema change to a table that has NULL values
	//       causes the NULL values to be removed and listed in the dolt_constraint_violation table, so running
	//       dolt_verify_constraints wouldn't find those violations since the NULL values aren't in the table anymore.
}
