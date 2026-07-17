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
	{
		// See https://github.com/dolthub/dolt/issues/11317
		Name: "ON DELETE CASCADE on a self-referential table with an indexed virtual generated column",
		SetUpScript: []string{
			"CREATE TABLE t2 (" +
				"id int PRIMARY KEY, " +
				"parentId int, " +
				"parentKey int AS (parentId) VIRTUAL, " +
				"UNIQUE KEY uk (parentKey), " +
				"FOREIGN KEY (parentId) REFERENCES t2(id) ON DELETE CASCADE);",
			"INSERT INTO t2 (id, parentId) VALUES (1, NULL), (2, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "DELETE FROM t2 WHERE id = 1;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "SELECT id, parentId, parentKey FROM t2 ORDER BY id;",
				// id=2 references id=1, so the cascade also removes it.
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT parentKey FROM t2 WHERE parentKey = 1;",
				// This lookup goes through the unique index, which must not keep a stale entry.
				Expected: []sql.Row{},
			},
			{
				Query: "INSERT INTO t2 (id, parentId) VALUES (3, NULL), (4, 3);",
				// Re-inserting a value freed by the cascade must succeed rather than hit a leftover index entry.
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT id, parentId, parentKey FROM t2 ORDER BY id;",
				Expected: []sql.Row{{3, nil, nil}, {4, 3, 3}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11317
		Name: "ON DELETE CASCADE with an indexed virtual column between stored columns",
		SetUpScript: []string{
			"CREATE TABLE parent (id int PRIMARY KEY);",
			"CREATE TABLE child (" +
				"id int PRIMARY KEY, " +
				"parentId int, " +
				"doubled int AS (parentId * 2) VIRTUAL, " +
				"note varchar(10), " +
				"KEY idx_doubled (doubled), " +
				"FOREIGN KEY (parentId) REFERENCES parent(id) ON DELETE CASCADE);",
			"INSERT INTO parent VALUES (1), (2);",
			"INSERT INTO child (id, parentId, note) VALUES (10, 1, 'a'), (20, 1, 'b'), (30, 2, 'c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "DELETE FROM parent WHERE id = 1;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "SELECT id, parentId, doubled, note FROM child ORDER BY id;",
				// Only the child rows referencing parent 1 are removed.
				Expected: []sql.Row{{30, 2, 4, "c"}},
			},
			{
				Query:    "SELECT id FROM child WHERE doubled = 4;",
				Expected: []sql.Row{{30}},
			},
			{
				Query:    "SELECT id FROM child WHERE doubled = 2;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11317
		Name: "ON DELETE SET NULL maintains an index over a virtual generated column",
		SetUpScript: []string{
			"CREATE TABLE t (" +
				"id int PRIMARY KEY, " +
				"parentId int, " +
				"parentKey int AS (parentId) VIRTUAL, " +
				"UNIQUE KEY uk (parentKey), " +
				"FOREIGN KEY (parentId) REFERENCES t(id) ON DELETE SET NULL);",
			"INSERT INTO t (id, parentId) VALUES (1, NULL), (2, 1), (3, 2);",
			"DELETE FROM t WHERE id = 2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT id, parentId, parentKey FROM t ORDER BY id;",
				Expected: []sql.Row{{1, nil, nil}, {3, nil, nil}},
			},
			{
				Query: "SELECT id FROM t WHERE parentKey = 2;",
				// The old value 2 must no longer be in the index.
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM t WHERE parentKey IS NULL;",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11317
		Name: "ON UPDATE CASCADE maintains a unique index over a virtual generated column",
		SetUpScript: []string{
			"CREATE TABLE parent (id int PRIMARY KEY);",
			"CREATE TABLE child (" +
				"id int PRIMARY KEY, " +
				"parentId int, " +
				"vcol int AS (parentId) VIRTUAL, " +
				"UNIQUE KEY uk (vcol), " +
				"FOREIGN KEY (parentId) REFERENCES parent(id) ON UPDATE CASCADE);",
			"INSERT INTO parent VALUES (2);",
			"INSERT INTO child (id, parentId) VALUES (20, 2);",
			"UPDATE parent SET id = 99 WHERE id = 2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT id, parentId, vcol FROM child ORDER BY id;",
				Expected: []sql.Row{{20, 99, 99}},
			},
			{
				Query:    "SELECT id FROM child WHERE vcol = 99;",
				Expected: []sql.Row{{20}},
			},
			{
				Query: "SELECT id FROM child WHERE vcol = 2;",
				// The old value 2 must no longer be in the index.
				Expected: []sql.Row{},
			},
			{
				Query: "INSERT INTO child (id, parentId) VALUES (40, 99);",
				// vcol=99 is already taken, so this insert must be rejected as a duplicate.
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11317
		Name: "ON UPDATE SET NULL maintains an index over a virtual generated column",
		SetUpScript: []string{
			"CREATE TABLE parent (id int PRIMARY KEY);",
			"CREATE TABLE child (" +
				"id int PRIMARY KEY, " +
				"parentId int, " +
				"vcol int AS (parentId) VIRTUAL, " +
				"UNIQUE KEY uk (vcol), " +
				"FOREIGN KEY (parentId) REFERENCES parent(id) ON UPDATE SET NULL);",
			"INSERT INTO parent VALUES (2);",
			"INSERT INTO child (id, parentId) VALUES (20, 2);",
			"UPDATE parent SET id = 99 WHERE id = 2;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT id, parentId, vcol FROM child ORDER BY id;",
				Expected: []sql.Row{{20, nil, nil}},
			},
			{
				Query: "SELECT id FROM child WHERE vcol = 2;",
				// The old value 2 must no longer be in the index.
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM child WHERE vcol IS NULL;",
				Expected: []sql.Row{{1}},
			},
		},
	},
}
