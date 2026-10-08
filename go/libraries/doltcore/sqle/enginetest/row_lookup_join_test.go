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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestRowLookupJoinConversions(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	for _, script := range rowLookupJoinConversionTests {
		enginetest.TestScript(t, h, script)
	}
}

func TestRowLookupJoinConversionsPrepared(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	for _, script := range rowLookupJoinConversionTests {
		enginetest.TestScriptPrepared(t, h, script)
	}
}

// Expected results were checked against MySQL 8.4. The LIMIT keeps the left
// side as SQL rows, and the hints request a lookup into the right-side index.
var rowLookupJoinConversionTests = []queries.ScriptTest{
	{
		Name: "signed narrowing",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v BIGINT)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v TINYINT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,-129),(2,-128),(3,0),(4,127),(5,128),(6,NULL),(7,1)`,
			`INSERT INTO rdst VALUES (1,-128),(2,0),(3,127),(4,NULL),(5,1)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{2, 1}, {3, 2}, {4, 3}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 1}, {3, 2}, {4, 3}, {5, nil}, {6, nil}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 1}, {3, 2}, {4, 3}, {5, nil}, {6, 4}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 2}, {4, 3}, {5, nil}, {6, nil}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}},
			},
		},
	},
	{
		Name: "signed to unsigned",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v BIGINT)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v TINYINT UNSIGNED, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,-1),(2,0),(3,255),(4,256),(5,NULL),(6,1)`,
			`INSERT INTO rdst VALUES (1,0),(2,255),(3,NULL),(4,1)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{2, 1}, {3, 2}, {6, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 1}, {3, 2}, {4, nil}, {5, nil}, {6, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 1}, {3, 2}, {4, nil}, {5, 3}, {6, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 2}, {4, nil}, {5, nil}, {6, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}},
			},
		},
	},
	{
		Name: "unsigned to signed",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v BIGINT UNSIGNED)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v BIGINT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,0),(2,9223372036854775807),(3,9223372036854775808),(4,18446744073709551615),(5,NULL),(6,1)`,
			`INSERT INTO rdst VALUES (1,-1),(2,0),(3,9223372036854775807),(4,NULL),(5,1)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 3}, {6, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, nil}, {4, nil}, {5, nil}, {6, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, nil}, {4, nil}, {5, 4}, {6, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, nil}, {4, nil}, {5, nil}, {6, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}},
			},
		},
	},
	{
		Name: "integer widening",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v INT)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v BIGINT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,-2147483648),(2,2147483647),(3,NULL),(4,1)`,
			`INSERT INTO rdst VALUES (1,-2147483648),(2,2147483647),(3,2147483648),(4,NULL),(5,1)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, nil}, {4, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 4}, {4, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, nil}, {4, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}},
			},
		},
	},
	{
		Name: "fractional decimal to integer",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v DECIMAL(12,3))`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v INT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,-1.5),(2,-1),(3,0.4),(4,0.5),(5,1),(6,1.5),(7,2),(8,NULL)`,
			`INSERT INTO rdst VALUES (1,-2),(2,-1),(3,0),(4,1),(5,2),(6,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{2, 2}, {5, 4}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, nil}, {4, nil}, {5, 4}, {6, nil}, {7, 5}, {8, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, nil}, {4, nil}, {5, 4}, {6, nil}, {7, 5}, {8, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, nil}, {4, nil}, {5, 4}, {6, nil}, {7, 5}, {8, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}, {8, nil}},
			},
		},
	},
	{
		Name: "fractional float to integer",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v DOUBLE)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v INT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,-1.5),(2,0.5),(3,1),(4,1.49),(5,1.5),(6,2),(7,NULL)`,
			`INSERT INTO rdst VALUES (1,-2),(2,0),(3,1),(4,2),(5,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{3, 3}, {6, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 3}, {4, nil}, {5, nil}, {6, 4}, {7, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 3}, {4, nil}, {5, nil}, {6, 4}, {7, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 3}, {4, nil}, {5, nil}, {6, 4}, {7, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}},
			},
		},
	},
	{
		Name: "string to integer",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v VARCHAR(40) COLLATE utf8mb4_0900_bin)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v INT, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'1'),(2,'01'),(3,'1x'),(4,'1.0'),(5,'1e0'),(6,'abc'),(7,''),(8,' 2 '),(9,NULL)`,
			`INSERT INTO rdst VALUES (1,0),(2,1),(3,2),(4,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}, {6, 1}, {7, 1}, {8, 3}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}, {6, 1}, {7, 1}, {8, 3}, {9, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}, {6, 1}, {7, 1}, {8, 3}, {9, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}, {6, nil}, {7, nil}, {8, 3}, {9, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}, {8, nil}, {9, nil}},
			},
		},
	},
	{
		Name: "string width",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v VARCHAR(40) COLLATE utf8mb4_0900_bin)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v VARCHAR(3) COLLATE utf8mb4_0900_bin, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'a'),(2,'abc'),(3,'abcd'),(4,''),(5,'é'),(6,NULL),(7,'b')`,
			`INSERT INTO rdst VALUES (1,'a'),(2,'abc'),(3,''),(4,'é'),(5,NULL),(6,'b')`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 3}, {5, 4}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, nil}, {4, 3}, {5, 4}, {6, nil}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, nil}, {4, 3}, {5, 4}, {6, 5}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, nil}, {4, 3}, {5, 4}, {6, nil}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}},
			},
		},
	},
	{
		Name: "collation",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v VARCHAR(20) COLLATE utf8mb4_0900_ai_ci)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v VARCHAR(20) COLLATE utf8mb4_0900_ai_ci, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'ALPHA'),(2,'é'),(3,'a'),(4,'missing'),(5,NULL)`,
			`INSERT INTO rdst VALUES (1,'alpha'),(2,'e'),(3,'A'),(4,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, nil}, {5, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, nil}, {5, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, 3}, {4, nil}, {5, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}},
			},
		},
	},
	{
		Name: "decimal rescale",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v DECIMAL(12,3))`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v DECIMAL(5,2), KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,1.234),(2,1.235),(3,1.230),(4,-1.235),(5,999.999),(6,NULL),(7,1.2)`,
			`INSERT INTO rdst VALUES (1,1.23),(2,1.24),(3,-1.24),(4,999.99),(5,NULL),(6,1.2)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{3, 1}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 1}, {4, nil}, {5, nil}, {6, nil}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, 1}, {4, nil}, {5, nil}, {6, 5}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}, {7, nil}},
			},
		},
	},
	{
		Name: "date to datetime",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v DATE)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v DATETIME(6), KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'2020-01-01'),(2,'2020-01-02'),(3,NULL)`,
			`INSERT INTO rdst VALUES (1,'2020-01-01 00:00:00'),(2,'2020-01-01 12:00:00'),(3,'2020-01-02 00:00:00'),(4,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 3}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 3}, {3, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}},
			},
		},
	},
	{
		Name: "datetime to date",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v DATETIME(6))`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v DATE, KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'2020-01-01 00:00:00'),(2,'2020-01-01 12:00:00'),(3,'2020-01-02 00:00:00.000001'),(4,NULL),(5,'2020-01-02 00:00:00')`,
			`INSERT INTO rdst VALUES (1,'2020-01-01'),(2,'2020-01-02'),(3,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {5, 2}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, nil}, {3, nil}, {4, nil}, {5, 2}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, nil}, {3, nil}, {4, 3}, {5, 2}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, 2}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}},
			},
		},
	},
	{
		Name: "enum to string",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v ENUM('a','b','c'))`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v VARCHAR(20), KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'a'),(2,'b'),(3,'c'),(4,NULL)`,
			`INSERT INTO rdst VALUES (1,'a'),(2,'b'),(3,'c'),(4,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, 3}, {4, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}},
			},
		},
	},
	{
		Name: "set to string",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v SET('a','b','c'))`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v VARCHAR(20), KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,'a'),(2,'b'),(3,'a,b'),(4,''),(5,NULL)`,
			`INSERT INTO rdst VALUES (1,'a'),(2,'b'),(3,'a,b'),(4,''),(5,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, 2}, {3, 3}, {4, 4}, {5, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}},
			},
		},
	},
	{
		Name: "integer to string",
		SetUpScript: []string{
			`CREATE TABLE lsrc (id INT PRIMARY KEY, v INT)`,
			`CREATE TABLE rdst (id INT PRIMARY KEY, v VARCHAR(20), KEY v_idx(v))`,
			`INSERT INTO lsrc VALUES (1,0),(2,1),(3,2),(4,NULL)`,
			`INSERT INTO rdst VALUES (1,'1'),(2,'01'),(3,'1.0'),(4,'abc'),(5,'2'),(6,NULL)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 4}, {2, 1}, {2, 2}, {2, 3}, {3, 5}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 4}, {2, 1}, {2, 2}, {2, 3}, {3, 5}, {4, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v <=> l.v
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 4}, {2, 1}, {2, 2}, {2, 3}, {3, 5}, {4, 6}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id > 1
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, 4}, {2, 2}, {2, 3}, {3, 5}, {4, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) JOIN_ORDER(l,r) */ l.id, r.id
FROM (SELECT id, v FROM lsrc LIMIT 1000) l
LEFT JOIN rdst r ON r.v = l.v AND r.id < 0
ORDER BY l.id, r.id`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}},
			},
		},
	},
	{
		Name: "composite keys and outer join state",
		SetUpScript: []string{
			`CREATE TABLE sources (id INT PRIMARY KEY, a BIGINT, b BIGINT)`,
			`CREATE TABLE targets (id INT PRIMARY KEY, a TINYINT, b TINYINT, payload VARCHAR(10), KEY ab(a,b))`,
			`INSERT INTO sources VALUES (1,1,2),(2,1,300),(3,1,NULL),(4,NULL,2),(5,1,2),(6,9,9)`,
			`INSERT INTO targets VALUES (1,1,2,'keep'),(2,1,2,'drop'),(3,1,NULL,'nullb'),(4,NULL,2,'nulla')`,
			`CREATE TABLE duplicates (a TINYINT, b TINYINT, payload VARCHAR(10), KEY ab(a,b))`,
			`INSERT INTO duplicates VALUES (1,2,'keep'),(1,2,'keep'),(1,2,'drop'),(1,NULL,'nullb')`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a=l.a AND r.b=l.b
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {2, nil}, {3, nil}, {4, nil}, {5, "drop"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a <=> l.a AND r.b <=> l.b
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {2, nil}, {3, "nullb"}, {4, "nulla"}, {5, "drop"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a=l.a AND r.b=l.b AND r.payload='keep'
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "keep"}, {2, nil}, {3, nil}, {4, nil}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a=l.a AND r.b=l.b AND r.payload='missing'
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a=l.a AND r.b=2
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {2, "drop"}, {2, "keep"}, {3, "drop"}, {3, "keep"}, {4, nil}, {5, "drop"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN targets r ON r.a=l.a
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {1, "nullb"}, {2, "drop"}, {2, "keep"}, {2, "nullb"}, {3, "drop"}, {3, "keep"}, {3, "nullb"}, {4, nil}, {5, "drop"}, {5, "keep"}, {5, "nullb"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a=l.a AND r.b=l.b
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {1, "keep"}, {2, nil}, {3, nil}, {4, nil}, {5, "drop"}, {5, "keep"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a <=> l.a AND r.b <=> l.b
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {1, "keep"}, {2, nil}, {3, "nullb"}, {4, nil}, {5, "drop"}, {5, "keep"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a=l.a AND r.b=l.b AND r.payload='keep'
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "keep"}, {1, "keep"}, {2, nil}, {3, nil}, {4, nil}, {5, "keep"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a=l.a AND r.b=l.b AND r.payload='missing'
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a=l.a AND r.b=2
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {1, "keep"}, {2, "drop"}, {2, "keep"}, {2, "keep"}, {3, "drop"}, {3, "keep"}, {3, "keep"}, {4, nil}, {5, "drop"}, {5, "keep"}, {5, "keep"}, {6, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.payload
FROM (SELECT * FROM sources LIMIT 1000) l
LEFT JOIN duplicates r ON r.a=l.a
ORDER BY l.id, r.payload`,
				Expected: []sql.Row{{1, "drop"}, {1, "keep"}, {1, "keep"}, {1, "nullb"}, {2, "drop"}, {2, "keep"}, {2, "keep"}, {2, "nullb"}, {3, "drop"}, {3, "keep"}, {3, "keep"}, {3, "nullb"}, {4, nil}, {5, "drop"}, {5, "keep"}, {5, "keep"}, {5, "nullb"}, {6, nil}},
			},
			{
				Query: `WITH grouped AS (SELECT a, COUNT(*) AS n FROM sources GROUP BY a)
SELECT /*+ LOOKUP_JOIN(g,r) */ g.a, g.n, r.id
FROM grouped g LEFT JOIN targets r ON r.a=g.a
ORDER BY g.a, r.id`,
				Expected: []sql.Row{{nil, 1, nil}, {1, 4, 1}, {1, 4, 2}, {1, 4, 3}, {9, 1, nil}},
			},
			{
				Query: `SELECT /*+ LOOKUP_JOIN(l,r) */ l.id, r.id
FROM (SELECT * FROM sources LIMIT 1000) l
JOIN targets r ON r.a=l.a AND r.b=l.b
ORDER BY l.id, r.id LIMIT 1`,
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
}
