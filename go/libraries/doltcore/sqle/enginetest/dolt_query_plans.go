// Copyright 2021 Dolthub, Inc.
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
)

// DoltDiffPlanTests are tests that check our query plans for various operations on the dolt diff system tables
var DoltDiffPlanTests = []queries.QueryPlanTest{
	{
		Query: `select * from dolt_diff_one_pk where to_pk=1`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk>=10 and to_pk<=100`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[10, 100]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1 and to_pk2=2`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1 < 1 and to_pk2 > 10`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{(-∞, 1), (10, ∞)}])\n" +
			"",
	},
}

var DoltDiffPlanNewFormatTests = []queries.QueryPlanTest{
	{
		Query: `select * from dolt_diff_one_pk where to_pk=1`,
		ExpectedPlan: "Exchange\n" +
			" └─ Filter(dolt_diff_one_pk.to_pk = 1)\n" +
			"     └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk>=10 and to_pk<=100`,
		ExpectedPlan: "Exchange\n" +
			" └─ Filter((dolt_diff_one_pk.to_pk >= 10) AND (dolt_diff_one_pk.to_pk <= 100))\n" +
			"     └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[10, 100]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1`,
		ExpectedPlan: "Exchange\n" +
			" └─ Filter(dolt_diff_two_pk.to_pk1 = 1)\n" +
			"     └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1 and to_pk2=2`,
		ExpectedPlan: "Exchange\n" +
			" └─ Filter((dolt_diff_two_pk.to_pk1 = 1) AND (dolt_diff_two_pk.to_pk2 = 2))\n" +
			"     └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1 < 1 and to_pk2 > 10`,
		ExpectedPlan: "Exchange\n" +
			" └─ Filter((dolt_diff_two_pk.to_pk1 < 1) AND (dolt_diff_two_pk.to_pk2 > 10))\n" +
			"     └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{(-∞, 1), (10, ∞)}])\n" +
			"",
	},
}

var NewFormatQueryPlanTests = []queries.QueryPlanTest{
	{
		Query: `SELECT * FROM one_pk ORDER BY pk`,
		ExpectedPlan: "Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1`,
		ExpectedPlan: "Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Project(two_pk.pk1 as one, two_pk.pk2 as two)\n" +
			" └─ Projected table access on [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY one, two`,
		ExpectedPlan: "Project(two_pk.pk1 as one, two_pk.pk2 as two)\n" +
			" └─ Projected table access on [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2 order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 < 2 AND v2 IS NOT NULL`,
		ExpectedPlan: "Filter((one_pk_two_idx.v1 < 2) AND (NOT(one_pk_two_idx.v2 IS NULL)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1,one_pk_two_idx.v2] with ranges: [{(-∞, 2), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 IN (1, 2) AND v2 <= 2`,
		ExpectedPlan: "Filter((one_pk_two_idx.v1 HASH IN (1, 2)) AND (one_pk_two_idx.v2 <= 2))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1,one_pk_two_idx.v2] with ranges: [{[2, 2], (-∞, 2]}, {[1, 1], (-∞, 2]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v2 = 3`,
		ExpectedPlan: "Filter((one_pk_three_idx.v1 > 2) AND (one_pk_three_idx.v2 = 3))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3] with ranges: [{(2, ∞), [3, 3], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v3 = 3`,
		ExpectedPlan: "Filter((one_pk_three_idx.v1 > 2) AND (one_pk_three_idx.v3 = 3))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3] with ranges: [{(2, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2
				where mytable.i = 2
				order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Filter(mytable.i = 2)\n" +
			"             │   └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Insert(i, s)\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project(t1.i, 'hello')\n" +
			"         └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"             ├─ Filter(t2.i = 1)\n" +
			"             │   └─ TableAlias(t2)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"             └─ Filter(t1.i = 2)\n" +
			"                 └─ TableAlias(t1)\n" +
			"                     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ InnerJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t1.i = 2)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(t1)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}])\n" +
			"     └─ Filter(t2.i = 1)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2, t3) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (mytable.s = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin((mytable.i = ot.i2) OR (mytable.s = ot.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, ' ', 1) = othertable.s2)) OR (SUBSTRING_INDEX(mytable.s, ' ', 2) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ Concat\n" +
			"         │   ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         │   └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Distinct\n" +
			" └─ Union\n" +
			"     ├─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │   └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │       ├─ Table(mytable)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ LeftJoin(sub.i = ot.i2)\n" +
			"     ├─ Filter(ot.i2 > 0)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{(0, ∞)}])\n" +
			"     └─ HashLookup(child: (sub.i), lookup: (ot.i2))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(sub)\n" +
			"                 └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"                     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"                         ├─ Table(mytable)\n" +
			"                         └─ Filter(NOT((convert(othertable.s2, signed) = 0)))\n" +
			"                             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "IndexedJoin(i.pk = j.pk)\n" +
			" ├─ IndexedJoin(i.pk = k.pk)\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ Table(one_pk)\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			" └─ HashLookup(child: (j.pk), lookup: (i.pk))\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias(j)\n" +
			"             └─ Project(one_pk.pk, RAND() as r)\n" +
			"                 └─ Projected table access on [pk]\n" +
			"                     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Insert()\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project((sub.i + 10), ot.s2)\n" +
			"         └─ IndexedJoin(sub.i = ot.i2)\n" +
			"             ├─ SubqueryAlias(sub)\n" +
			"             │   └─ Project(mytable.i)\n" +
			"             │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             │           ├─ Table(mytable)\n" +
			"             │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project(mytable.i, selfjoin.i)\n" +
			" └─ Filter(selfjoin.i IN (Project(1)\n" +
			"     └─ Table(dual)\n" +
			"    ))\n" +
			"     └─ IndexedJoin(mytable.i = selfjoin.i)\n" +
			"         ├─ Table(mytable)\n" +
			"         └─ TableAlias(selfjoin)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project(othertable.s2, othertable.i2, mytable.i)\n" +
			"     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(othertable)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 <=> mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 = mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT(concat(mytable.s, othertable.s2) IS NULL)))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "InnerJoin((mytable.i = othertable.i2) AND (mytable.s > othertable.s2))\n" +
			" ├─ Projected table access on [i s]\n" +
			" │   └─ Table(mytable)\n" +
			" └─ Projected table access on [s2 i2]\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "InnerJoin((mytable.i = othertable.i2) AND (NOT((mytable.s > othertable.s2))))\n" +
			" ├─ Projected table access on [i s]\n" +
			" │   └─ Table(mytable)\n" +
			" └─ Projected table access on [s2 i2]\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ InnerJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable LEFT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ LeftJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM (SELECT * FROM mytable) mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ HashLookup(child: (mytable.i), lookup: (othertable.i2))\n" +
			"     │   └─ CachedResults\n" +
			"     │       └─ SubqueryAlias(mytable)\n" +
			"     │           └─ Projected table access on [i s]\n" +
			"     │               └─ Table(mytable)\n" +
			"     └─ SubqueryAlias(othertable)\n" +
			"         └─ Projected table access on [s2 i2]\n" +
			"             └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a WHERE a.s is not null`,
		ExpectedPlan: "Filter(NOT(a.s IS NULL))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(<nil>, ∞)}, {(-∞, <nil>)}])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT(a.s IS NULL))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(<nil>, ∞)}, {(-∞, <nil>)}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ Filter(NOT(a.s IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT((a.s HASH IN ('1', '2', '3', '4'))))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(1, 2)}, {(2, 3)}, {(3, 4)}, {(4, ∞)}, {(-∞, 1)}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i HASH IN (1, 2, 3, 4))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (1, 2, 3, 4))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (NULL, 2, 3, 4))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[<nil>, <nil>]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1+2)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (3))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[3, 3]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where upper(s) IN ('FIRST ROW', 'SECOND ROW')`,
		ExpectedPlan: "Filter(UPPER(mytable.s) HASH IN ('FIRST ROW', 'SECOND ROW'))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('a', 'b')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN ('a', 'b'))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('1', '2')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN ('1', '2'))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i > 2) IN (true)`,
		ExpectedPlan: "Filter((mytable.i > 2) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 6) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 6) HASH IN (7, 8))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 40) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 40) HASH IN (7, 8))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 | false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 1) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 & false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 0) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (2*i)`,
		ExpectedPlan: "Filter(mytable.i IN ((2 * mytable.i)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (i)`,
		ExpectedPlan: "Filter(mytable.i IN (mytable.i))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE 4 IN (i + 2)`,
		ExpectedPlan: "Filter(4 IN ((mytable.i + 2)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (cast('first row' AS CHAR))`,
		ExpectedPlan: "Filter(mytable.s HASH IN ('first row'))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{[first row, first row]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')`,
		ExpectedPlan: "Filter(mytable.s HASH IN ('second row', 'FIRST ROW'))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{[FIRST ROW, FIRST ROW]}, {[second row, second row]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where true IN (i > 3)`,
		ExpectedPlan: "Filter(true IN ((mytable.i > 3)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.s = b.i OR a.i = 1`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.s = b.i) OR (a.i = 1))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i >= b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = a.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.s)\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i HASH IN (2, 432, 7))\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[432, 432]}, {[7, 7]}, {[2, 2]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ Projected table access on [s]\n" +
			"         └─ TableAlias(d)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = b.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin((a.i = b.i) OR (a.i = b.s))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = a.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.i)\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ Projected table access on [s]\n" +
			"         └─ TableAlias(d)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.s = c.s)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [s i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [s]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i BETWEEN 10 AND 20)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[10, 20]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		ExpectedPlan: "Sort(lefttable.i ASC)\n" +
			" └─ Project(lefttable.i, righttable.s)\n" +
			"     └─ InnerJoin((lefttable.i = righttable.i) AND (righttable.s = lefttable.s))\n" +
			"         ├─ SubqueryAlias(lefttable)\n" +
			"         │   └─ Projected table access on [i s]\n" +
			"         │       └─ Table(mytable)\n" +
			"         └─ HashLookup(child: (righttable.i, righttable.s), lookup: (lefttable.i, lefttable.s))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(righttable)\n" +
			"                     └─ Projected table access on [i s]\n" +
			"                         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightIndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.s2 = 'a')\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{[a, a]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_three)\n" +
			" └─ SubqueryAlias(othertable_two)\n" +
			"     └─ SubqueryAlias(othertable_one)\n" +
			"         └─ Filter(othertable.s2 = 'a')\n" +
			"             └─ Projected table access on [s2 i2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{[a, a]}])\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Filter(othertable.s2 > 'a')\n" +
			"     │       └─ Projected table access on [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{(a, ∞)}])\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Limit(1)\n" +
			" └─ Project(othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Project(othertable.i2)\n" +
			" └─ Projected table access on [i2]\n" +
			"     └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable WHERE mytable.i = othertable.i2)`,
		ExpectedPlan: "Filter(mytable.i IN (Project(othertable.i2)\n" +
			" └─ Filter(mytable.i = othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"))\n" +
			" └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ Filter(mt.i > 2)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{(2, ∞)}])\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "IndexedJoin((mt.i = o.pk) AND (mt.s = o.c2))\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable)\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = (othertable.i2 - 1))\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "CrossJoin\n" +
			" ├─ Table(tabletest)\n" +
			" └─ IndexedJoin(mt.i = ot.i2)\n" +
			"     ├─ TableAlias(mt)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project(t1.Timestamp)\n" +
			" └─ IndexedJoin(t1.Timestamp = t2.Timestamp)\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table(reservedWordsTable)\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ IndexedTableAccess(reservedWordsTable on [reservedWordsTable.Timestamp])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 OR one_pk.c2 = two_pk.c3`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin(((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2)) OR (one_pk.c2 = two_pk.c3))\n" +
			"     ├─ Projected table access on [pk c2]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c3]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2`,
		ExpectedPlan: "Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			" └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Filter(othertable.i2 = 1)\n" +
			"         └─ Projected table access on [i2 s2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC`,
		ExpectedPlan: "Sort(datetime_table.date_col ASC)\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ TopN(Limit: [100]; datetime_table.date_col ASC)\n" +
			"     └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"         └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100 OFFSET 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ Offset(100)\n" +
			"     └─ TopN(Limit: [(100 + 100)]; datetime_table.date_col ASC)\n" +
			"         └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"             └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col = '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col > '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col = '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col > '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col = '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col > '2020-01-01')\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.timestamp_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.date_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.datetime_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i ASC)\n" +
			" └─ Project(dt1.i)\n" +
			"     └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"         ├─ TableAlias(dt2)\n" +
			"         │   └─ Table(datetime_table)\n" +
			"         └─ TableAlias(dt1)\n" +
			"             └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3 offset 0`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ Offset(0)\n" +
			"     └─ TopN(Limit: [(3 + 0)]; dt1.i ASC)\n" +
			"         └─ Project(dt1.i)\n" +
			"             └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"                 ├─ TableAlias(dt2)\n" +
			"                 │   └─ Table(datetime_table)\n" +
			"                 └─ TableAlias(dt1)\n" +
			"                     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ TopN(Limit: [3]; dt1.i ASC)\n" +
			"     └─ Project(dt1.i)\n" +
			"         └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"             ├─ TableAlias(dt2)\n" +
			"             │   └─ Table(datetime_table)\n" +
			"             └─ TableAlias(dt1)\n" +
			"                 └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, tpk.pk1, tpk2.pk1, tpk.pk2, tpk2.pk2)\n" +
			"     └─ IndexedJoin(((one_pk.pk - 1) = tpk2.pk1) AND (one_pk.pk = tpk2.pk2))\n" +
			"         ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND ((one_pk.pk - 1) = tpk.pk2))\n" +
			"         │   ├─ Table(one_pk)\n" +
			"         │   └─ TableAlias(tpk)\n" +
			"         │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk 
						RIGHT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						RIGHT JOIN two_pk tpk2 ON tpk.pk1=TPk2.pk2 AND tpk.pk2=TPK2.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ RightIndexedJoin((tpk.pk1 = tpk2.pk2) AND (tpk.pk2 = tpk2.pk1))\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "Project(mytable.i, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(((mytable.i - 1) = two_pk.pk1) AND ((mytable.i - 2) = two_pk.pk2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk 
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i + 1`,
		ExpectedPlan: "Project(one_pk.pk, nt.i, nt2.i)\n" +
			" └─ RightIndexedJoin(one_pk.pk = (nt2.i + 1))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = nt.i)\n" +
			"         ├─ TableAlias(nt)\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = niltable.i) AND (NOT(niltable.f IS NULL)))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [i f]\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.c1 > 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.pk > 1)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(1, ∞)}])\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((b.pk1 = a.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(((a.pk1 + 1) = b.pk1) AND ((a.pk2 + 1) = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(NOT(niltable.f IS NULL))\n" +
			"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(one_pk.pk > 1)\n" +
			"         │   └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(1, ∞)}])\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(one_pk.pk > 0)\n" +
			"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(niltable)\n" +
			"             └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [i f]\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin((two_pk.pk1 - one_pk.pk) > 0)\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Filter(two_pk.pk2 < 1)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Projected table access on [pk c1]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2 c1]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Projected table access on [pk c1]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2 c1]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Filter(one_pk.c1 = 10)\n" +
			"     │   └─ Projected table access on [pk c1]\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c1]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ Projected table access on [pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ Project(t1.pk, t2.pk1, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter(mytable.i = mt.i)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ Filter(mytable.i > 2)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{(2, ∞)}])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter(othertable.i2 = mt.i)\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i and i > 2) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter(mytable.i = mt.i)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter((othertable.i2 = mt.i) AND (mt.i > 2))\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2, (Limit(1)\n" +
			"     └─ Project(one_pk.pk)\n" +
			"         └─ Projected table access on [pk]\n" +
			"             └─ Filter(one_pk.pk = 1)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"    ) as (SELECT pk from one_pk where pk = 1 limit 1))\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE s2 <> 'second' ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"     └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"         └─ Filter(NOT((othertable.s2 = 'second')))\n" +
			"             └─ Projected table access on [i2 s2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{(second, ∞)}, {(-∞, second)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter(NOT((othertable.s2 = 'second')))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"             └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE i2 < 2 OR i2 > 2 ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"     └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"         └─ Filter((othertable.i2 < 2) OR (othertable.i2 > 2))\n" +
			"             └─ Projected table access on [i2 s2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{(-∞, 2)}, {(2, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE i2 < 2 OR i2 > 2`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter((othertable.i2 < 2) OR (othertable.i2 > 2))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"             └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Projected table access on [i2 s2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT t, n, lag(t, 1, t+1) over (partition by n) FROM bigtable`,
		ExpectedPlan: "Project(bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n) as lag(t, 1, t+1) over (partition by n))\n" +
			" └─ Window(bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n))\n" +
			"     └─ Projected table access on [t n]\n" +
			"         └─ Table(bigtable)\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w3) from mytable window w1 as (w2), w2 as (), w3 as (w1)`,
		ExpectedPlan: "Project(mytable.i, row_number() over () as row_number() over (w3))\n" +
			" └─ Window(mytable.i, row_number() over ())\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w1 partition by s) from mytable window w1 as (order by i asc)`,
		ExpectedPlan: "Project(mytable.i, row_number() over ( partition by mytable.s order by [mytable.i, idx=0, type=BIGINT, nullable=false] ASC) as row_number() over (w1 partition by s))\n" +
			" └─ Window(mytable.i, row_number() over ( partition by mytable.s order by [mytable.i, idx=0, type=BIGINT, nullable=false] ASC))\n" +
			"     └─ Projected table access on [i s]\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE c1 > 1`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter(two_pk.c1 > 1)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE c1 > 1`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter(two_pk.c1 > 1)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `UPDATE /*+ JOIN_ORDER(two_pk, one_pk) */ one_pk JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET two_pk.c1 = (two_pk.c1 + 1))\n" +
			"         └─ Project(one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, two_pk.pk1, two_pk.pk2, two_pk.c1, two_pk.c2, two_pk.c3, two_pk.c4, two_pk.c5)\n" +
			"             └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"                 ├─ Table(two_pk)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET one_pk.c1 = (one_pk.c1 + 1),SET one_pk.c2 = (one_pk.c2 + 1))\n" +
			"         └─ Project(one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, t2.pk1, t2.pk2, t2.c1, t2.c2, t2.c3, t2.c4, t2.c5)\n" +
			"             └─ IndexedJoin(one_pk.pk = t2.pk1)\n" +
			"                 ├─ SubqueryAlias(t2)\n" +
			"                 │   └─ Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"                 │       └─ Table(two_pk)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z`,
		ExpectedPlan: "Project(a.x, a.y, a.z)\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z AND a.z = 2`,
		ExpectedPlan: "Project(a.x, a.y, a.z)\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     └─ Filter(a.z = 2)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		ExpectedPlan: "Filter(invert_pk.y = 0)\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, 0], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		ExpectedPlan: "Filter(invert_pk.y >= 0)\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		ExpectedPlan: "Filter((invert_pk.y >= 0) AND (invert_pk.z < 1))\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, ∞), (-∞, 1), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk WHERE pk IN (1)`,
		ExpectedPlan: "Filter(one_pk.pk HASH IN (1))\n" +
			" └─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c LEFT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ LeftIndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c RIGHT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ RightJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table(one_pk)\n" +
			"     │   └─ Projected table access on [pk]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ IndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk b INNER JOIN one_pk c ON b.pk = c.pk LEFT JOIN one_pk d ON c.pk = d.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ LeftIndexedJoin(c.pk = d.pk)\n" +
			"     ├─ IndexedJoin(b.pk = c.pk)\n" +
			"     │   ├─ CrossJoin\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(one_pk)\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN (select * from one_pk) b ON b.pk = c.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ InnerJoin(b.pk = c.pk)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ HashLookup(child: (b.pk), lookup: (c.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(b)\n" +
			"                 └─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"                     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i ASC, mt.i ASC, ot.i2 ASC)\n" +
			" └─ IndexedJoin(tabletest.i = ot.i2)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Table(tabletest)\n" +
			"     │   └─ TableAlias(mt)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0;`,
		ExpectedPlan: "Project(a.pk, c.v2)\n" +
			" └─ Filter(b.pk = 0)\n" +
			"     └─ RightJoin(b.pk = c.v1)\n" +
			"         ├─ CrossJoin\n" +
			"         │   ├─ Projected table access on [pk]\n" +
			"         │   │   └─ TableAlias(a)\n" +
			"         │   │       └─ Table(one_pk_three_idx)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(b)\n" +
			"         │           └─ Table(one_pk_three_idx)\n" +
			"         └─ Filter(c.v2 = 0)\n" +
			"             └─ Projected table access on [v2 v1]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table(one_pk_three_idx)\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project(a.pk, c.v2)\n" +
			" └─ LeftIndexedJoin(b.pk = c.v1)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Filter(a.v2 = 1)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table(one_pk_three_idx)\n" +
			"     │   └─ Filter(b.pk = 0)\n" +
			"     │       └─ TableAlias(b)\n" +
			"     │           └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk] with ranges: [{[0, 0]}])\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3])\n" +
			"",
	},
	{
		Query: `with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;`,
		ExpectedPlan: "RightJoin((a.i + 1) = (c.i - 1))\n" +
			" ├─ CachedResults\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Project(a.i, a.s)\n" +
			" │           └─ CrossJoin\n" +
			" │               ├─ Projected table access on [i s]\n" +
			" │               │   └─ TableAlias(a)\n" +
			" │               │       └─ Table(mytable)\n" +
			" │               └─ TableAlias(b)\n" +
			" │                   └─ Table(mytable)\n" +
			" └─ TableAlias(c)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ RightIndexedJoin(b.i = d.i)\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"         ├─ RightIndexedJoin(a.i = (b.i + 1))\n" +
			"         │   ├─ TableAlias(b)\n" +
			"         │   │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         │   └─ TableAlias(a)\n" +
			"         │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project(a.i, a.s, b.s2, b.i2)\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ Table(othertable)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project(a.i, a.s, b.s2, b.i2)\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ RightIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   └─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │       ├─ TableAlias(b)\n" +
			"     │       │   └─ Table(othertable)\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;`,
		ExpectedPlan: "Project(i.pk, j.v3)\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project(i.pk, j.v3)\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project(a.pk, a.v1, a.v2)\n" +
			" └─ LeftIndexedJoin(a.pk = l.v2)\n" +
			"     ├─ RightIndexedJoin(a.pk = i.v1)\n" +
			"     │   ├─ IndexedJoin(i.v1 = j.pk)\n" +
			"     │   │   ├─ TableAlias(i)\n" +
			"     │   │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   │   └─ TableAlias(j)\n" +
			"     │   │       └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.pk])\n" +
			"     └─ IndexedJoin(k.v1 = l.pk)\n" +
			"         ├─ TableAlias(k)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(l)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;`,
		ExpectedPlan: "Project(a.pk, a.v1, a.v2)\n" +
			" └─ RightIndexedJoin(a.v1 = l.v2)\n" +
			"     ├─ IndexedJoin(k.v2 = l.v3)\n" +
			"     │   ├─ TableAlias(k)\n" +
			"     │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   └─ TableAlias(l)\n" +
			"     │       └─ Table(one_pk_three_idx)\n" +
			"     └─ LeftIndexedJoin(a.pk = i.pk)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1])\n" +
			"         └─ IndexedJoin(i.pk = j.v3)\n" +
			"             ├─ TableAlias(j)\n" +
			"             │   └─ Table(one_pk_three_idx)\n" +
			"             └─ TableAlias(i)\n" +
			"                 └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.pk])\n" +
			"",
	},
}
