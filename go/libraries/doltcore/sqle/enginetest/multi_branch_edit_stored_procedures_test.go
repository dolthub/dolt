// Copyright 2022 Dolthub, Inc.
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

func TestMergeRejectsDirtyWorkingSets(t *testing.T) {
	for _, args := range []string{"'b1'", "'--no-ff', 'b1'", "'--squash', 'b1'", "'--no-commit', 'b1'"} {
		for _, dirty := range []string{"mydb", "mydb/b2", "other"} {
			t.Run(args+"/"+dirty, func(t *testing.T) {
				h := newDoltHarness(t)
				defer h.Close()
				enginetest.TestScript(t, h, queries.ScriptTest{
					Name: "merge rejects pending transaction edits before changing HEAD or working sets",
					SetUpScript: []string{
						"create table t (pk int primary key)", "call dolt_commit('-Am', 'setup')",
						"call dolt_branch('b1')", "call dolt_branch('b2')",
						"use mydb/b1", "insert into t values (1)", "call dolt_commit('-am', 'advance')",
						"create database other", "create table other.t (pk int primary key)",
						"use mydb", "set autocommit=0",
						"insert into `" + dirty + "`.t values (9)",
					},
					Assertions: []queries.ScriptTestAssertion{
						{Query: "call dolt_merge(" + args + ")", ExpectedErrStr: "cannot merge with uncommitted changes"},
						{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{}},
						{Query: "select * from `" + dirty + "`.t", Expected: []sql.Row{{9}}},
						{Query: "select is_merging from dolt_merge_status", Expected: []sql.Row{{false}}},
						{Query: "rollback", SkipResultsCheck: true},
						{Query: "select * from `" + dirty + "`.t", Expected: []sql.Row{}},
					},
				})
			})
		}
	}
}

func TestMergeAbortRejectsOtherDirtyWorkingSets(t *testing.T) {
	for _, dirty := range []string{"mydb/b2", "other"} {
		t.Run(dirty, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, queries.ScriptTest{
				Name: "abort checks other working sets before discarding target changes",
				SetUpScript: []string{
					"create table t (pk int primary key, v int)", "insert into t values (1, 0)",
					"call dolt_commit('-Am', 'setup')", "call dolt_branch('b1')", "call dolt_branch('b2')",
					"use mydb/b1", "update t set v=1", "call dolt_commit('-am', 'branch change')",
					"use mydb", "update t set v=2", "call dolt_commit('-am', 'main change')",
					"create database other", "create table other.t (pk int primary key, v int)",
					"set autocommit=0", "call dolt_merge('b1')",
					"insert into `" + dirty + "`.t values (9, 9)",
				},
				Assertions: []queries.ScriptTestAssertion{
					{Query: "call dolt_merge('--abort')", ExpectedErrStr: "cannot merge with uncommitted changes"},
					{Query: "select count(*) from dolt_conflicts", Expected: []sql.Row{{1}}},
					{Query: "select * from t as of 'HEAD'", Expected: []sql.Row{{1, 2}}},
					{Query: "select * from `" + dirty + "`.t where pk=9", Expected: []sql.Row{{9, 9}}},
					{Query: "rollback", SkipResultsCheck: true},
					{Query: "select * from `" + dirty + "`.t where pk=9", Expected: []sql.Row{}},
					{Query: "call dolt_merge('b1')", SkipResultsCheck: true},
					{Query: "update t set v=99", SkipResultsCheck: true},
					{Query: "call dolt_merge('--abort')", Expected: []sql.Row{{"", 0, 0, "merge aborted"}}},
					{Query: "select * from t", Expected: []sql.Row{{1, 2}}},
					{Query: "select count(*) from dolt_conflicts", Expected: []sql.Row{{0}}},
				},
			})
		})
	}
}
