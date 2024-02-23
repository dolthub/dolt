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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

var SchemaOverrideTests = []queries.ScriptTest{
	// TODO: Add more tests with different projection changes (no values, PK not at front of row, etc)

	// BASIC CASES
	{
		Name: "Basic Case: Dropping a column and adding a column",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t (pk, c1) values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t drop column c1;",
			"call dolt_commit('-am', 'dropping column c1 on main');",
			"SET @commit2 = hashof('HEAD');",

			"alter table t add column c2 varchar(255);",
			"insert into t (pk, c2) values (2, 'two');",
			"call dolt_commit('-am', 'adding column c2 on main');",
			"SET @commit3 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schemas
				Query:    "SET @@dolt_override_schema=@commit3;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, nil}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// use the first commit from main for our response schema (pk, c1)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, nil}, {2, nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// Test retrieving a subset of the schema
				Query:    "select pk from t;",
				Expected: []sql.Row{{1}, {2}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
				},
			},
			{
				// Test retrieving the full schema, plus an extra literal column
				Query:    "select pk, 42, c1 from t;",
				Expected: []sql.Row{{1, 42, nil}, {2, 42, nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "42",
						Type: gmstypes.Int8,
					},
					{
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// turn off the schema override
				Query:    "SET @@dolt_override_schema=NULL;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, nil}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},
	{
		Name: "Basic Case: Adding columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t add column c2 varchar(255);",
			"insert into t values (2, 'two', 'zwei');",
			"call dolt_commit('-am', 'adding column c2 on main');",
			"SET @commit2 = hashof('HEAD');",

			"alter table t add column c3 varchar(255);",
			"insert into t values (3, 'three', 'drei', 'tres');",
			"call dolt_commit('-am', 'adding column c3 on main');",
			"SET @commit3 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schema (pk, c1, c2, c3)
				Query:    "SET @@dolt_override_schema=@commit3;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one", nil, nil}, {2, "two", "zwei", nil}, {3, "three", "drei", "tres"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "c3",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// use the previous commit from main for our response schema (pk, c1, c2)
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one", nil}, {2, "two", "zwei"}, {3, "three", "drei"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// use the first commit from main for our response schemas (pk, c1)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}, {3, "three"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},
	{
		Name: "Basic Case: Dropping columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(10), c2 int NOT NULL, c3 float, c4 text, c5 int, c6 varchar(10), c7 int unsigned);",
			"insert into t values (1, 'one', 2, 3.0, 'four', 5, 'six', 7);",
			"call dolt_commit('-Am', 'adding table t');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t drop column c5;",
			"call dolt_commit('-am', 'dropping column c5');",
			"SET @commit2 = hashof('HEAD');",

			"alter table t drop column c2;",
			"call dolt_commit('-am', 'dropping column c2');",
			"SET @commit3 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use @commit2 response schema (pk, c1, c2, c3, c4, c6, c7)
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one", nil, 3.0, "four", "six", uint32(7)}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "c2",
						Type: gmstypes.Int32,
					}, {
						Name: "c3",
						Type: gmstypes.Float32,
					}, {
						Name: "c4",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.Text, 255),
					}, {
						Name: "c6",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 10),
					}, {
						Name: "c7",
						Type: gmstypes.Uint32,
					},
				},
			},
			{
				// use @commit2 response schema (pk, c1, c2, c3, c4, c5, c6, c7)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one", nil, 3.0, "four", nil, "six", uint32(7)}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "c2",
						Type: gmstypes.Int32,
					}, {
						Name: "c3",
						Type: gmstypes.Float32,
					}, {
						Name: "c4",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.Text, 255),
					}, {
						Name: "c5",
						Type: gmstypes.Int32,
					}, {
						Name: "c6",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 10),
					}, {
						Name: "c7",
						Type: gmstypes.Uint32,
					},
				},
			},
		},
	},

	{
		Name: "Basic Case: Renaming a column",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t rename column c1 to c2;",
			"insert into t values (2, 'two');",
			"call dolt_commit('-am', 'renaming column c1 to c2 on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schema (pk, c2)
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},

	// PK CHANGES
	{
		Name: "PK Change: Adding a column to PK",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t add column pk2 int;",
			"update t set pk2 = 1;",
			"alter table t drop primary key, add primary key(pk, pk2);",
			"insert into t values (2, 'two', 2);",
			"call dolt_commit('-am', 'adding a column to the PK');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the first commit for our response schema (pk, c1)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},
	{
		Name: "PK Change: Dropping a column from PK",
		SetUpScript: []string{
			"create table t (pk int, c1 varchar(255), pk2 int, primary key(pk, pk2));",
			"insert into t values (1, 'one', 1);",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t drop primary key, add primary key (pk);",
			"alter table t drop column pk2;",
			"insert into t values (2, 'two');",
			"call dolt_commit('-am', 'dropping a column from the PK');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the first commit for our response schema (pk, c1)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one", nil}, {2, "two", nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "pk2",
						Type: gmstypes.Int32,
					},
				},
			},
		},
	},

	// TYPE CHANGES
	{
		Name: "Type Change: Compatible data",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t modify column c1 TEXT;",
			"insert into t values (2, 'two');",
			"call dolt_commit('-am', 'adding column c2 on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schemas
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.Text, 255),
					},
				},
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},
	{
		Name: "Type Change: Incompatible data",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(5));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding tables t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t modify column c1 varchar(100);",
			"insert into t values (2, 'twotwotwotwotwotwo');",
			"call dolt_commit('-am', 'modifying columns in t on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schemas (int, varchar(100))
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "twotwotwotwotwotwo"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 100),
					},
				},
			},
			{
				// go back to the first commit, where the schema had a more narrow type (int, varchar(5))
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:          "select * from t;",
				ExpectedErrStr: "unable to convert value to overridden schema: string 'twotwotwotwotwotwo' is too large for column 'varchar(5)'",
			},
		},
	},

	// TABLE EXISTENCE
	{
		Name: "Table Existence: Table exists in the data commit, NOT in pinned schema commit",
		SetUpScript: []string{
			"SET @commit1 = hashof('HEAD');",
			"create table addedTable (pk int primary key, c1 varchar(255));",
			"insert into addedTable values (1, 'one');",
			"call dolt_commit('-Am', 'adding table addedTable on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from addedTable;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:          "SELECT * from addedTable;",
				ExpectedErrStr: "unable to find table at overridden schema root",
			},
		},
	},
	{
		Name: "Table Existence: Table exists in the pinned schema commit, NOT in data commit",
		SetUpScript: []string{
			"create table deletedTable (pk int primary key, c1 varchar(255));",
			"insert into deletedTable values (1, 'one');",
			"call dolt_commit('-Am', 'adding table deletedTable on main');",
			"SET @commit1 = hashof('HEAD');",

			"drop table deletedTable;",
			"call dolt_commit('-Am', 'deleting table deletedTable on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * from deletedTable;",
				ExpectedErrStr: "table not found: deletedtable",
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "SELECT * from deletedTable;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Table Existence: Table exists in the pinned schema commit, NOT in AS OF data commit",
		SetUpScript: []string{
			"create table deletedTable (pk int primary key, c1 varchar(255));",
			"insert into deletedTable values (1, 'one');",
			"call dolt_commit('-Am', 'adding table deletedTable on main');",
			"SET @commit1 = hashof('HEAD');",

			"drop table deletedTable;",
			"call dolt_commit('-Am', 'deleting table deletedTable on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * from deletedTable;",
				ExpectedErrStr: "table not found: deletedtable",
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "SELECT * from deletedTable AS OF @commit2;",
				Expected: []sql.Row{},
			},
		},
	},

	// INDEXES
	{
		Name: "Indexes: Index in the pinned schema, but not in the data commit",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255), key c1_idx(c1));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t with index on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t drop index c1_idx;",
			"update t set c1='two';",
			"call dolt_commit('-Am', 'adding table t with index on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Going back to @commit1 with AS OF should use the available index
				Query:           "SELECT c1 from t as of @commit1 where c1 > 'o';",
				Expected:        []sql.Row{{"one"}},
				ExpectedIndexes: []string{"c1_idx"},
			},
			{
				// The tip of HEAD does not have an index
				Query:           "SELECT c1 from t where c1 > 'o';",
				Expected:        []sql.Row{{"two"}},
				ExpectedIndexes: []string{},
			},
			{
				// Set the overridden schema to the point where an index existed
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				// Using the overridden index, we should still get the latest data, but without using the index
				Query:           "SELECT c1 from t where c1 > 'o';",
				Expected:        []sql.Row{{"two"}},
				ExpectedIndexes: []string{},
			},
			{
				// Set the overridden schema to the point where an index existed
				Query:    "SET @@dolt_override_schema=@commit2;",
				Expected: []sql.Row{{}},
			},
			{
				// Going back to @commit1 for data, but using @commit2 for schema
				Query:           "SELECT c1 from t as of @commit1 where c1 > 'o';",
				Expected:        []sql.Row{{"one"}},
				ExpectedIndexes: []string{"c1_idx"},
			},
		},
	},

	// AS OF TEST CASES
	{
		Name: "AS OF: schema pinning with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(255));",
			"insert into t (pk, c1) values (1, 'one');",
			"call dolt_commit('-Am', 'adding table t on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t drop column c1;",
			"call dolt_commit('-am', 'dropping column c1 on main');",
			"SET @commit2 = hashof('HEAD');",

			"alter table t add column c2 varchar(255);",
			"insert into t (pk, c2) values (2, 'two');",
			"call dolt_commit('-am', 'adding column c2 on main');",
			"SET @commit3 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schemas
				Query:    "SET @@dolt_override_schema=@commit3;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t as of @commit1;",
				Expected: []sql.Row{{1, nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				// use the previous commit from main for our response schemas
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select * from t as of @commit2;",
				Expected: []sql.Row{{1, nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
			{
				Query:    "select * from t as of @commit3;",
				Expected: []sql.Row{{1, nil}, {2, nil}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					},
					{
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},

	// JOIN TEST CASES
	{
		Name: "Joins: Two tables with changed schemas",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c1 varchar(255));",
			"create table t2 (pk int primary key, c1 int, c2 varchar(100));",
			"insert into t1 values (1, 'one');",
			"insert into t2 values (100, 1, 'blue');",
			"call dolt_commit('-Am', 'adding tables t1 and t2 on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t1 rename column c1 to c2;",
			"alter table t2 modify column c1 varchar(100);",
			"call dolt_commit('-am', 'modifying columns in t1 and t2 on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// use the tip of main for our response schema (pk, c2)
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "SELECT * from t1 JOIN t2 on t1.pk = t2.c1;",
				Expected: []sql.Row{{1, "one", 100, 1, "blue"}},
				ExpectedColumns: sql.Schema{
					{
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					}, {
						Name: "pk",
						Type: gmstypes.Int32,
					}, {
						Name: "c1",
						Type: gmstypes.Int32,
					}, {
						Name: "c2",
						Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
					},
				},
			},
		},
	},

	// READ-ONLY TEST CASES
	{
		// When a schema override is applied, the database is read-only
		Name: "Read-only: Write queries are not allowed when the schema is overridden",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c1 varchar(5));",
			"insert into t1 values (1, 'one');",
			"call dolt_commit('-Am', 'adding tables t1 and t2 on main');",
			"SET @commit1 = hashof('HEAD');",

			"alter table t1 modify column c1 varchar(100);",
			"call dolt_commit('-am', 'modifying columns in t1 and t2 on main');",
			"SET @commit2 = hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Before @@dolt_override_schema is applied, we can executed DDL and update/insert statements
				Query:    "create table t2 (pk int primary key, c1 JSON);",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:    "SET @@dolt_override_schema=@commit1;",
				Expected: []sql.Row{{}},
			},
			{
				// After @@dolt_override_schema is applied, DDL statements error out
				Query:          "create table t3 (pk int primary key, c1 JSON);",
				ExpectedErrStr: "Database mydb is read-only.",
			},
			{
				// After @@dolt_override_schema is applied, DDL statements error out
				Query:          "insert into t1 values (3, NULL);",
				ExpectedErrStr: "Database mydb is read-only.",
			},
			{
				// Turn off the schema override
				Query:    "SET @@dolt_override_schema=NULL;",
				Expected: []sql.Row{{}},
			},
			{
				// Insert statements work again after turning off the schema override
				Query:    "insert into t1 values (3, NULL);",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
		},
	},
}
