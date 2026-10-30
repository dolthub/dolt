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

// pendingEditsDerivingStatements read the whole working set or derive a new one from it. Each must include edits
// that are pending when it runs.
var pendingEditsDerivingStatements = []string{
	"create table n (id int primary key)",
	"alter table t add column z int",
	"alter table t modify column y bigint",
	"alter table t drop column y",
	"create index yi on t (y)",
	"alter table t rename to t2",
	"rename table t to t2",
	"create view v as select * from t",
	"create trigger trg before insert on o for each row set new.id = new.id",
	"create procedure p() select 1",
	"truncate table o",
	"drop table o",
	"alter table t add constraint chk check (x > 0)",
	"alter table o add foreign key (id) references t (x)",
	"call dolt_add('.')",
	"call dolt_commit('-Am', 'c')",
	"call dolt_branch('b2')",
	"select count(*) from dolt_status",
	"select count(*) from dolt_diff_t",
	"select dolt_hashof_db()",
	"savepoint s",
}

func TestPendingEditsSurviveDerivedRoots(t *testing.T) {
	for _, q := range pendingEditsDerivingStatements {
		t.Run(q, func(t *testing.T) {
			tblName := "t"
			if q == "alter table t rename to t2" || q == "rename table t to t2" {
				tblName = "t2"
			}
			script := queries.TransactionTest{
				Name:        q,
				SetUpScript: []string{"create table t (x int primary key, y int)", "create table o (id int primary key)", "insert into o values (1)", "call dolt_commit('-Am', 'init')"},
				Assertions: []queries.ScriptTestAssertion{
					{Query: "/* client a */ start transaction", Expected: []sql.Row{}},
					{Query: "/* client a */ insert into t (x) values (1), (2)", SkipResultsCheck: true},
					{Query: "/* client a */ " + q, SkipResultsCheck: true},
					{Query: "/* client a */ select count(*) from " + tblName, Expected: []sql.Row{{2}}},
					{Query: "/* client a */ commit", Expected: []sql.Row{}},
					{Query: "/* client b */ select count(*) from " + tblName, Expected: []sql.Row{{2}}},
				},
			}
			h := newDoltHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		})
	}
}
