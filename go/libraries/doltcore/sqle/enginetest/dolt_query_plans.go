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
			"     └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], [NULL, ∞)}])\n" +
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
			"     └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{(NULL, 1), (10, ∞)}])\n" +
			"",
	},
}
