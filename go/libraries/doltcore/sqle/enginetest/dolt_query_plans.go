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
		// See https://github.com/dolthub/dolt/issues/11159
		Query: `select * from dolt_diff_one_pk where to_commit='abc' and to_pk=1`,
		// A non-unique to_commit index is not a point lookup, so the planner prefers the primary key index.
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_one_pk.to_commit = 'abc') AND (dolt_diff_one_pk.to_pk = 1))\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk)\n" +
			"     ├─ index: [dolt_diff_one_pk.to_pk]\n" +
			"     └─ filters: [{[1, 1]}]\n" +
			"",
	},
	{
		Query: `select a.to_pk from dolt_diff_one_pk a join dolt_diff_one_pk b on a.from_commit = b.from_commit`,
		// A non-unique from_commit index can match many rows per key, so the planner chooses a hash join over a lookup join.
		ExpectedPlan: "Project\n" +
			" ├─ columns: [a.to_pk]\n" +
			" └─ HashJoin\n" +
			"     ├─ (a.from_commit = b.from_commit)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table\n" +
			"     │       └─ name: dolt_diff_one_pk\n" +
			"     └─ HashLookup\n" +
			"         ├─ left-key: (a.from_commit)\n" +
			"         ├─ right-key: (b.from_commit)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table\n" +
			"                 └─ name: dolt_diff_one_pk\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk=1`,
		ExpectedPlan: "Filter\n" +
			" ├─ (dolt_diff_one_pk.to_pk = 1)\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk)\n" +
			"     ├─ index: [dolt_diff_one_pk.to_pk]\n" +
			"     └─ filters: [{[1, 1]}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk>=10 and to_pk<=100`,
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_one_pk.to_pk >= 10) AND (dolt_diff_one_pk.to_pk <= 100))\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk)\n" +
			"     ├─ index: [dolt_diff_one_pk.to_pk]\n" +
			"     └─ filters: [{[10, 100]}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where from_pk>=10 and from_pk<=100`,
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_one_pk.from_pk >= 10) AND (dolt_diff_one_pk.from_pk <= 100))\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk)\n" +
			"     ├─ index: [dolt_diff_one_pk.from_pk]\n" +
			"     └─ filters: [{[10, 100]}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1`,
		ExpectedPlan: "Filter\n" +
			" ├─ (dolt_diff_two_pk.to_pk1 = 1)\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk)\n" +
			"     ├─ index: [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2]\n" +
			"     └─ filters: [{[1, 1], [NULL, ∞)}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1 and to_pk2=2`,
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_two_pk.to_pk1 = 1) AND (dolt_diff_two_pk.to_pk2 = 2))\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk)\n" +
			"     ├─ index: [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2]\n" +
			"     └─ filters: [{[1, 1], [2, 2]}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1 < 1 and to_pk2 > 10`,
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_two_pk.to_pk1 < 1) AND (dolt_diff_two_pk.to_pk2 > 10))\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk)\n" +
			"     ├─ index: [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2]\n" +
			"     └─ filters: [{(NULL, 1), (10, ∞)}]\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where from_pk1 < 1 and from_pk2 = 10`,
		ExpectedPlan: "Filter\n" +
			" ├─ ((dolt_diff_two_pk.from_pk1 < 1) AND (dolt_diff_two_pk.from_pk2 = 10))\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk)\n" +
			"     ├─ index: [dolt_diff_two_pk.from_pk1,dolt_diff_two_pk.from_pk2]\n" +
			"     └─ filters: [{(NULL, 1), [10, 10]}]\n" +
			"",
	},
}

var DoltCommitPlanTests = []queries.QueryPlanTest{
	{
		Query: "select * from dolt_log order by commit_hash;",
		ExpectedPlan: "Sort(dolt_log.commit_hash ASC)\n" +
			" └─ Table\n" +
			"     ├─ name: dolt_log\n" +
			"     └─ columns: [commit_hash committer email date message commit_order parents refs signature author author_email author_date]\n" +
			"",
	},
	{
		Query: "select * from dolt_diff order by commit_hash;",
		ExpectedPlan: "Sort(dolt_diff.commit_hash ASC)\n" +
			" └─ Table\n" +
			"     └─ name: dolt_diff\n" +
			"",
	},
	{
		Query: "select * from dolt_commits order by commit_hash;",
		ExpectedPlan: "Sort(dolt_commits.commit_hash ASC)\n" +
			" └─ Table\n" +
			"     └─ name: dolt_commits\n" +
			"",
	},
	{
		Query: "select * from dolt_commit_ancestors order by commit_hash;",
		ExpectedPlan: "Sort(dolt_commit_ancestors.commit_hash ASC)\n" +
			" └─ Table\n" +
			"     └─ name: dolt_commit_ancestors\n" +
			"",
	},
}
