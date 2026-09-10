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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestTagNameIndex(t *testing.T) {
	testRefNameIndex(t, "dolt_tags", "tag_name", "dolt_tag", "dolt_tags_name_idx")
}

func TestBranchNameIndex(t *testing.T) {
	testRefNameIndex(t, "dolt_branches", "name", "dolt_branch", "dolt_branches_name_idx")
}

func testRefNameIndex(t *testing.T, table, column, procedure, index string) {
	script := queries.ScriptTest{Name: table + " name index", SetUpScript: []string{
		"create table wanted (name varchar(100) collate utf8mb4_0900_bin)",
		"insert into wanted values ('alpha'), ('gamma'), ('missing'), (NULL), ('alpha')",
	}}
	if table == "dolt_branches" {
		// Rename the initial branch so both system tables have the same fixture.
		script.SetUpScript = append(script.SetUpScript, "call dolt_branch('-m','main','Zebra')")
	}
	for _, name := range []string{"Zebra", "alpha", "beta", "gamma", "prefix/one", "zulu"} {
		if table == "dolt_branches" && name == "Zebra" {
			continue
		}
		script.SetUpScript = append(script.SetUpScript, fmt.Sprintf("call %s('%s')", procedure, name))
	}
	tests := []struct {
		predicate string
		names     []string
	}{
		{"= 'alpha'", []string{"alpha"}},
		{"= 'missing'", nil},
		{"= 'ALPHA'", nil},
		{"> 'beta'", []string{"gamma", "prefix/one", "zulu"}},
		{">= 'beta'", []string{"beta", "gamma", "prefix/one", "zulu"}},
		{"< 'beta'", []string{"Zebra", "alpha"}},
		{"<= 'beta'", []string{"Zebra", "alpha", "beta"}},
		{"between 'alpha' and 'gamma'", []string{"alpha", "beta", "gamma"}},
		{"in ('alpha', 'gamma', 'missing', 'alpha')", []string{"alpha", "gamma"}},
		{"in ('alpha', NULL)", []string{"alpha"}},
		{"< ''", nil},
		{"= ''", nil},
		{">= 'zulu'", []string{"zulu"}},
		{"<> 'beta'", []string{"Zebra", "alpha", "gamma", "prefix/one", "zulu"}},
	}
	for _, test := range tests {
		rows := make([]sql.Row, len(test.names))
		for i, name := range test.names {
			rows[i] = sql.Row{name}
		}
		script.Assertions = append(script.Assertions, queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select %s from %s where %s %s order by %s", column, table, column, test.predicate, column),
			Expected:        rows,
			ExpectedIndexes: []string{index},
		})
	}
	script.Assertions = append(script.Assertions,
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select %s from %s where %s > 'alpha' and %s < 'gamma'", column, table, column, column),
			Expected:        []sql.Row{{"beta"}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select %s from %s where %s between 'alpha' and 'gamma' or %s between 'beta' and 'gamma' order by %s", column, table, column, column, column),
			Expected:        []sql.Row{{"alpha"}, {"beta"}, {"gamma"}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select %s from %s where %s = (select name from wanted where name = 'gamma')", column, table, column),
			Expected:        []sql.Row{{"gamma"}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select %s from %s where %s in (select name from wanted) order by %s", column, table, column, column),
			Expected:        []sql.Row{{"alpha"}, {"gamma"}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select /*+ LOOKUP_JOIN(w, r) */ r.%s from wanted w join %s r on w.name = r.%s order by r.%s", column, table, column, column),
			Expected:        []sql.Row{{"alpha"}, {"alpha"}, {"gamma"}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:           fmt.Sprintf("select /*+ LOOKUP_JOIN(w, r) */ w.name, r.%s from wanted w left join %s r on w.name = r.%s order by w.name", column, table, column),
			Expected:        []sql.Row{{nil, nil}, {"alpha", "alpha"}, {"alpha", "alpha"}, {"gamma", "gamma"}, {"missing", nil}},
			ExpectedIndexes: []string{index}},
		queries.ScriptTestAssertion{
			Query:    fmt.Sprintf("select %s from %s where %s is null", column, table, column),
			Expected: []sql.Row{}},
		queries.ScriptTestAssertion{
			Query: fmt.Sprintf("explain format=tree select %s from %s where %s = 'alpha'", column, table, column),
			Expected: []sql.Row{
				{"Project"},
				{fmt.Sprintf(" ├─ columns: [%s.%s]", table, column)},
				{fmt.Sprintf(" └─ IndexedTableAccess(%s)", table)},
				{fmt.Sprintf("     ├─ index: [%s.%s]", table, column)},
				{"     └─ filters: [{[alpha, alpha]}]"},
			}},
	)
	hashColumn := "tag_hash"
	if table == "dolt_branches" {
		hashColumn = "hash"
	}
	script.Assertions = append(script.Assertions, queries.ScriptTestAssertion{
		Query:           fmt.Sprintf("select %s, %s = hashof('alpha') from %s where %s = 'alpha'", column, hashColumn, table, column),
		Expected:        []sql.Row{{"alpha", true}},
		ExpectedIndexes: []string{index},
	})
	// Re-executing a prepared plan must see refs created since its previous execution.
	script.SetUpScript = append(script.SetUpScript, fmt.Sprintf("prepare ref_lookup from 'select %s from %s where %s = \"new_ref\"'", column, table, column))
	script.Assertions = append(script.Assertions,
		queries.ScriptTestAssertion{
			Query:    "execute ref_lookup",
			Expected: []sql.Row{}},
		queries.ScriptTestAssertion{
			Query:    fmt.Sprintf("call %s('new_ref')", procedure),
			Expected: []sql.Row{{0}}},
		queries.ScriptTestAssertion{
			Query:    "execute ref_lookup",
			Expected: []sql.Row{{"new_ref"}}},
		queries.ScriptTestAssertion{
			Query:    fmt.Sprintf("call %s('-d','new_ref')", procedure),
			Expected: []sql.Row{{0}}},
		queries.ScriptTestAssertion{
			Query:    "execute ref_lookup",
			Expected: []sql.Row{}},
	)
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			h := newDoltEnginetestHarness(t)
			defer h.Close()
			if prepared {
				enginetest.TestScriptPrepared(t, h, script)
			} else {
				enginetest.TestScript(t, h, script)
			}
		})
	}
}
