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
	"github.com/dolthub/go-mysql-server/sql"
)

var MultiBranchWorkingSetTransactionTests = []queries.TransactionTest{
	{
		Name: "three clients merge partially overlapping branch sets",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b2`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client c */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client c */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "disjoint updates and deletes merge on overlapping branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/main`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ delete from `mydb/b1`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b2`.t set y=222 where pk=2",
				SkipResultsCheck: true,
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
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "different columns of the same row merge on two branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "late branch conflict rolls back the complete batch and permits retry",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set x=12 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ commit",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 12, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=13 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "delete update conflict does not publish nonconflicting branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ commit",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "identical edits merge while preserving independent branch writes",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "savepoint restores edits across branches before merging another client",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=99 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ rollback to savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ release savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "rollback and no-op commit leave other clients branch writes intact",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:            "/* client a */ rollback",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
}

var MultiBranchHeadTransactionTests = []queries.TransactionTest{
	{
		Name: "three clients merge partially overlapping branch sets",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b2`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client c */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b2", doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client c */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client c */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b2", doltCommit},
					{"b3", doltCommit},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
					{6, 60, 600},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{6, 60, 600},
				},
			},
		},
	},
	{
		Name: "disjoint updates and deletes merge on overlapping branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/main`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ delete from `mydb/b1`.t where pk=3",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b2`.t set y=222 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b2", doltCommit},
				},
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 222},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "different columns of the same row merge on two branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set y=111 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 111},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "late branch conflict rolls back the complete batch and permits retry",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/main`.t set x=12 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b3", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query:       "/* client a */ call dolt_commit_all('-am', 'batch')",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 12, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=13 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 13, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "delete update conflict does not publish nonconflicting branches",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b2`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b3`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b3", doltCommit},
				},
			},
			{
				Query:       "/* client a */ call dolt_commit_all('-am', 'batch')",
				ExpectedErr: sql.ErrLockDeadlock,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "identical edits merge while preserving independent branch writes",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ update `mydb/b1`.t set x=11 where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b2", doltCommit},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 11, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client b */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
		},
	},
	{
		Name: "savepoint restores edits across branches before merging another client",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b3`.t values (6,60,600)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ delete from `mydb/b1`.t where pk=1",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ update `mydb/main`.t set x=99 where pk=2",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b2", doltCommit},
				},
			},
			{
				Query:            "/* client a */ rollback to savepoint keep",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"main", doltCommit},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{4, 40, 400},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b3`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
		},
	},
	{
		Name: "rollback and no-op commit leave other clients branch writes intact",
		SetUpScript: []string{
			"create table t (pk int primary key, x int, y int)",
			"insert into t values (1,10,100),(2,20,200),(3,30,300)",
			"call dolt_commit('-Am', 'setup')",
			"call dolt_branch('b1')",
			"call dolt_branch('b2')",
			"call dolt_branch('b3')",
			"set autocommit = 0",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "/* client a */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/b1`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ insert into `mydb/main`.t values (4,40,400)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b1`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ insert into `mydb/b2`.t values (5,50,500)",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client b */ call dolt_commit_all('-am', 'batch')",
				Expected: []sql.Row{
					{"b1", doltCommit},
					{"b2", doltCommit},
				},
			},
			{
				Query:            "/* client a */ rollback",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b1`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/main`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
				},
			},
			{
				Query: "/* client a */ select * from `mydb/b2`.t as of 'HEAD' order by pk",
				Expected: []sql.Row{
					{1, 10, 100},
					{2, 20, 200},
					{3, 30, 300},
					{5, 50, 500},
				},
			},
		},
	},
}
