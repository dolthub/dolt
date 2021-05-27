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
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var DoltTransactionTests = []enginetest.TransactionTest{
	{
		Name: "duplicate inserts, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
		},
	},
	{
		Name: "duplicate inserts, autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "conflicting inserts",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				ExpectedErrStr: "conflict in table t",
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			// TODO: behavior right now is to leave the session state dirty on an unsuccessful commit, letting the
			//  client choose whether to rollback or not. Not clear if this is the right behavior for a failed commit.
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 3}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "duplicate updates, autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(0),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 0,
					},
				}}},
			},
		},
	},
	{
		Name: "duplicate updates, autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ update t set y = 2",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 2}, {2, 2}},
			},
		},
	},
	{
		Name: "conflicting updates",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ update t set y = 3",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client b */ update t set y = 4",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				ExpectedErrStr: "conflict in table t",
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 4}, {2, 4}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
		},
	},
	{
		Name: "conflicting updates, resolved with more updates",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ update t set y = 3",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client b */ update t set y = 4",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				ExpectedErrStr: "conflict in table t",
			},
			{
				Query:    "/* client b */ update t set y = 3",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(2),
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 3}},
			},
		},
	},
	{
		Name: "non overlapping updates (diff rows)",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ update t set y = 3 where x = 1",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ update t set y = 4 where x = 2",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: uint64(1),
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 3}, {2, 4}},
			},
		},
	},
}
