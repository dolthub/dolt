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

package enginetest

import (
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

// LongVarcharPKScripts tests that tables with very long varchar primary keys maintain
// correct row ordering when scanned via the primary index. Queries that ORDER BY the
// primary key can skip an explicit sort step because the storage layer is supposed to
// return rows in key order. These tests verify that assumption holds for key values
// well over 5 000 characters, covering single-column (~9 000 char) and composite
// two- and three-column (~20 000 and ~30 000 chars total) configurations.
var LongVarcharPKScripts = []queries.ScriptTest{
	{
		Name: "single varchar(10000) primary key ordering with long values",
		SetUpScript: []string{
			`CREATE TABLE t_long_pk1 (pk varchar(10000) PRIMARY KEY)`,
			// Insert in reverse lexicographic order so that ORDER BY must actually reorder.
			`INSERT INTO t_long_pk1 VALUES (REPEAT('c', 9000))`,
			`INSERT INTO t_long_pk1 VALUES (REPEAT('a', 9000))`,
			`INSERT INTO t_long_pk1 VALUES (REPEAT('b', 9000))`,
			`INSERT INTO t_long_pk1 VALUES (REPEAT('d', 9000))`,
			`INSERT INTO t_long_pk1 VALUES (REPEAT('e', 9000))`,
			`INSERT INTO t_long_pk1 VALUES (REPEAT('f', 9000))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk FROM t_long_pk1 ORDER BY pk",
				Expected: []sql.Row{
					{strings.Repeat("a", 9000)},
					{strings.Repeat("b", 9000)},
					{strings.Repeat("c", 9000)},
					{strings.Repeat("d", 9000)},
					{strings.Repeat("e", 9000)},
					{strings.Repeat("f", 9000)},
				},
			},
		},
	},
	{
		Name: "two-column composite varchar(10000) primary key ordering with long values",
		// CHARACTER SET ascii (1 byte/char) keeps the per-row max storage at 20 000 bytes,
		// well within Dolt's 65 504-byte tuple limit while giving 20 000 characters total.
		SetUpScript: []string{
			`CREATE TABLE t_long_pk2 (
				pk1 varchar(10000) CHARACTER SET ascii,
				pk2 varchar(10000) CHARACTER SET ascii,
				PRIMARY KEY (pk1, pk2)
			)`,
			`INSERT INTO t_long_pk2 VALUES (REPEAT('b', 10000), REPEAT('b', 10000))`,
			`INSERT INTO t_long_pk2 VALUES (REPEAT('a', 10000), REPEAT('z', 10000))`,
			`INSERT INTO t_long_pk2 VALUES (REPEAT('a', 10000), REPEAT('a', 10000))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk1, pk2 FROM t_long_pk2 ORDER BY pk1, pk2",
				Expected: []sql.Row{
					{strings.Repeat("a", 10000), strings.Repeat("a", 10000)},
					{strings.Repeat("a", 10000), strings.Repeat("z", 10000)},
					{strings.Repeat("b", 10000), strings.Repeat("b", 10000)},
				},
			},
		},
	},
	{
		Name: "three-column composite varchar(10000) primary key ordering with long values",
		// Total key length per row: 3 × 10 000 = 30 000 characters / bytes (ascii charset).
		SetUpScript: []string{
			`CREATE TABLE t_long_pk3 (
				pk1 varchar(10000) CHARACTER SET ascii,
				pk2 varchar(10000) CHARACTER SET ascii,
				pk3 varchar(10000) CHARACTER SET ascii,
				PRIMARY KEY (pk1, pk2, pk3)
			)`,
			`INSERT INTO t_long_pk3 VALUES (REPEAT('b', 10000), REPEAT('b', 10000), REPEAT('b', 10000))`,
			`INSERT INTO t_long_pk3 VALUES (REPEAT('a', 10000), REPEAT('z', 10000), REPEAT('z', 10000))`,
			`INSERT INTO t_long_pk3 VALUES (REPEAT('a', 10000), REPEAT('a', 10000), REPEAT('z', 10000))`,
			`INSERT INTO t_long_pk3 VALUES (REPEAT('a', 10000), REPEAT('a', 10000), REPEAT('a', 10000))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk1, pk2, pk3 FROM t_long_pk3 ORDER BY pk1, pk2, pk3",
				Expected: []sql.Row{
					{strings.Repeat("a", 10000), strings.Repeat("a", 10000), strings.Repeat("a", 10000)},
					{strings.Repeat("a", 10000), strings.Repeat("a", 10000), strings.Repeat("z", 10000)},
					{strings.Repeat("a", 10000), strings.Repeat("z", 10000), strings.Repeat("z", 10000)},
					{strings.Repeat("b", 10000), strings.Repeat("b", 10000), strings.Repeat("b", 10000)},
				},
			},
		},
	},
}
