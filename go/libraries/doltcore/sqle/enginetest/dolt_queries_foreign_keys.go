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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DoltForeignKeyTests are Dolt-specific tests for foreign key behavior, separate from the
// engine-level foreign key tests in go-mysql-server.
var DoltForeignKeyTests = []queries.ScriptTest{
	{
		// See https://github.com/dolthub/dolt/issues/10903
		Name: "UPDATE on table with BINARY primary key, explicit secondary index, and foreign key",
		SetUpScript: []string{
			"CREATE TABLE a (a_id INT NOT NULL PRIMARY KEY);",
			"CREATE TABLE b (b_id BINARY(1) NOT NULL PRIMARY KEY, a_id INT NOT NULL, val INT NOT NULL, KEY a_id (a_id), CONSTRAINT FOREIGN KEY (a_id) REFERENCES a (a_id));",
			"INSERT INTO a (a_id) VALUES (1);",
			"INSERT INTO b (b_id, a_id, val) VALUES (0x3c, 1, 0), (0x52, 1, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "UPDATE b SET val = 2;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query:    "SELECT val FROM b ORDER BY b_id;",
				Expected: []sql.Row{{int32(2)}, {int32(2)}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/10903
		Name: "UPDATE on table with BINARY or VARBINARY primary key and foreign key",
		SetUpScript: []string{
			"CREATE TABLE parent (id INT NOT NULL PRIMARY KEY);",
			"CREATE TABLE bin_child (id BINARY(16) NOT NULL PRIMARY KEY, parent_id INT, val INT, CONSTRAINT fk_bin FOREIGN KEY (parent_id) REFERENCES parent(id));",
			"CREATE TABLE varbin_child (id VARBINARY(16) NOT NULL PRIMARY KEY, parent_id INT, val INT, CONSTRAINT fk_varbin FOREIGN KEY (parent_id) REFERENCES parent(id));",
			"INSERT INTO parent VALUES (1), (2);",
			"INSERT INTO bin_child VALUES (0x00000000000000000000000000000001, 1, 10);",
			"INSERT INTO varbin_child VALUES (0x0000000000000001, 1, 10);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "UPDATE bin_child SET val = 10 WHERE id = 0x00000000000000000000000000000001;",
				// 0 rows affected when the updated value matches the existing value.
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}}},
			},
			{
				Query:    "UPDATE bin_child SET val = 20 WHERE id = 0x00000000000000000000000000000001;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT val FROM bin_child WHERE id = 0x00000000000000000000000000000001;",
				Expected: []sql.Row{{int32(20)}},
			},
			{
				Query:    "UPDATE bin_child SET parent_id = 2 WHERE id = 0x00000000000000000000000000000001;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT parent_id FROM bin_child WHERE id = 0x00000000000000000000000000000001;",
				Expected: []sql.Row{{int32(2)}},
			},
			{
				Query:    "UPDATE varbin_child SET val = 20 WHERE id = 0x0000000000000001;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT val FROM varbin_child WHERE id = 0x0000000000000001;",
				Expected: []sql.Row{{int32(20)}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/10903
		Name: "UPDATE on table with composite BINARY primary key and foreign key",
		SetUpScript: []string{
			"CREATE TABLE parent (id INT NOT NULL PRIMARY KEY);",
			"CREATE TABLE child (id1 BINARY(8) NOT NULL, id2 BINARY(8) NOT NULL, parent_id INT, val INT, PRIMARY KEY (id1, id2), CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id));",
			"INSERT INTO parent VALUES (1);",
			"INSERT INTO child VALUES (0x0000000000000001, 0x0000000000000002, 1, 10);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "UPDATE child SET val = 20 WHERE id1 = 0x0000000000000001 AND id2 = 0x0000000000000002;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT val FROM child WHERE id1 = 0x0000000000000001 AND id2 = 0x0000000000000002;",
				Expected: []sql.Row{{int32(20)}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/10903
		Name: "UPDATE on table where the FK column is BINARY",
		SetUpScript: []string{
			"CREATE TABLE parent (id BINARY(16) NOT NULL PRIMARY KEY);",
			"CREATE TABLE child (id INT NOT NULL PRIMARY KEY, parent_id BINARY(16), val INT, CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id));",
			"INSERT INTO parent VALUES (0x00000000000000000000000000000001);",
			"INSERT INTO child VALUES (1, 0x00000000000000000000000000000001, 10);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "UPDATE child SET val = 20 WHERE id = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT val FROM child WHERE id = 1;",
				Expected: []sql.Row{{int32(20)}},
			},
		},
	},
}
