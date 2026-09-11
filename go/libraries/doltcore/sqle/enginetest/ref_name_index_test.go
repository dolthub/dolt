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

func TestTagNameIndex(t *testing.T) {
	script := queries.ScriptTest{
		Name: "dolt_tags name index",
		SetUpScript: []string{"create table wanted (name varchar(100) collate utf8mb4_0900_bin)",
			"insert into wanted values ('alpha'), ('gamma'), ('missing'), (NULL), ('alpha')",
			"call dolt_tag('Zebra')",
			"call dolt_tag('alpha')",
			"call dolt_tag('beta')",
			"call dolt_tag('gamma')",
			"call dolt_tag('prefix/one')",
			"call dolt_tag('zulu')",
			"prepare ref_lookup from 'select tag_name from dolt_tags where tag_name = \"new_ref\"' ",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:           "select tag_name from dolt_tags where tag_name = 'alpha' order by tag_name",
				Expected:        []sql.Row{{"alpha"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name = 'missing' order by tag_name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name = 'ALPHA' order by tag_name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name > 'beta' order by tag_name",
				Expected:        []sql.Row{{"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name >= 'beta' order by tag_name",
				Expected:        []sql.Row{{"beta"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name < 'beta' order by tag_name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name <= 'beta' order by tag_name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}, {"beta"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name between 'alpha' and 'gamma' order by tag_name",
				Expected:        []sql.Row{{"alpha"}, {"beta"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name in ('alpha','gamma','missing','alpha') order by tag_name",
				Expected:        []sql.Row{{"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name in ('alpha',NULL) order by tag_name",
				Expected:        []sql.Row{{"alpha"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name < '' order by tag_name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name = '' order by tag_name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name <> 'beta' order by tag_name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name between 'alpha' and 'gamma' or tag_name between 'beta' and 'zulu' order by tag_name",
				Expected:        []sql.Row{{"alpha"}, {"beta"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name = (select name from wanted where name = 'gamma')",
				Expected:        []sql.Row{{"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name in (select name from wanted) order by tag_name",
				Expected:        []sql.Row{{"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select /*+ LOOKUP_JOIN(w, r) */ r.tag_name from wanted w join dolt_tags r on w.name = r.tag_name order by r.tag_name",
				Expected:        []sql.Row{{"alpha"}, {"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select /*+ LOOKUP_JOIN(w, r) */ w.name, r.tag_name from wanted w left join dolt_tags r on w.name = r.tag_name order by w.name",
				Expected:        []sql.Row{{nil, nil}, {"alpha", "alpha"}, {"alpha", "alpha"}, {"gamma", "gamma"}, {"missing", nil}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:    "select tag_name from dolt_tags where tag_name is null",
				Expected: []sql.Row{},
			},
			{
				Query:    "select tag_name from dolt_tags where tag_name <=> NULL",
				Expected: []sql.Row{},
			},
			{
				Query:    "select tag_name from dolt_tags where tag_name not in ('alpha',NULL)",
				Expected: []sql.Row{},
			},
			{
				Query:           "select tag_name from dolt_tags order by tag_name desc limit 3",
				Expected:        []sql.Row{{"zulu"}, {"prefix/one"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name in ('alpha','gamma','beta') order by tag_name desc",
				Expected:        []sql.Row{{"gamma"}, {"beta"}, {"alpha"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name from dolt_tags where tag_name > 'alpha' and tag_name < 'gamma' order by tag_name",
				Expected:        []sql.Row{{"beta"}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query:           "select tag_name,tag_hash=hashof('alpha') from dolt_tags where tag_name='alpha'",
				Expected:        []sql.Row{{"alpha", true}},
				ExpectedIndexes: []string{"dolt_tags_name_idx"},
			},
			{
				Query: "explain format=tree select tag_name from dolt_tags where tag_name='alpha'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_tags.tag_name]"},
					{" └─ IndexedTableAccess(dolt_tags)"},
					{"     ├─ index: [dolt_tags.tag_name]"},
					{"     └─ filters: [{[alpha, alpha]}]"},
				},
			},
			{
				Query: "explain format=tree select tag_name from dolt_tags where tag_name between 'alpha' and 'gamma' order by tag_name desc",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_tags.tag_name]"},
					{" └─ IndexedTableAccess(dolt_tags)"},
					{"     ├─ index: [dolt_tags.tag_name]"},
					{"     ├─ filters: [{[alpha, gamma]}]"},
					{"     └─ reverse: true"},
				},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_tag('new_ref')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{{"new_ref"}},
			},
			{
				Query:    "call dolt_tag('-d','new_ref')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{},
			},
		},
	}
	t.Run("queries", func(t *testing.T) {
		h := newDoltEnginetestHarness(t)
		defer h.Close()
		enginetest.TestScript(t, h, script)
	})
	t.Run("prepared", func(t *testing.T) {
		h := newDoltEnginetestHarness(t)
		defer h.Close()
		enginetest.TestScriptPrepared(t, h, script)
	})
}

func TestBranchNameIndex(t *testing.T) {
	script := queries.ScriptTest{
		Name: "dolt_branches name index",
		SetUpScript: []string{"create table wanted (name varchar(100) collate utf8mb4_0900_bin)",
			"insert into wanted values ('alpha'), ('gamma'), ('missing'), (NULL), ('alpha')",
			"call dolt_branch('-m','main','Zebra')",
			"call dolt_branch('alpha')",
			"call dolt_branch('beta')",
			"call dolt_branch('gamma')",
			"call dolt_branch('prefix/one')",
			"call dolt_branch('zulu')",
			"prepare ref_lookup from 'select name from dolt_branches where name = \"new_ref\"' ",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:           "select name from dolt_branches where name = 'alpha' order by name",
				Expected:        []sql.Row{{"alpha"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name = 'missing' order by name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name = 'ALPHA' order by name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name > 'beta' order by name",
				Expected:        []sql.Row{{"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name >= 'beta' order by name",
				Expected:        []sql.Row{{"beta"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name < 'beta' order by name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name <= 'beta' order by name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}, {"beta"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name between 'alpha' and 'gamma' order by name",
				Expected:        []sql.Row{{"alpha"}, {"beta"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name in ('alpha','gamma','missing','alpha') order by name",
				Expected:        []sql.Row{{"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name in ('alpha',NULL) order by name",
				Expected:        []sql.Row{{"alpha"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name < '' order by name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name = '' order by name",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name <> 'beta' order by name",
				Expected:        []sql.Row{{"Zebra"}, {"alpha"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name between 'alpha' and 'gamma' or name between 'beta' and 'zulu' order by name",
				Expected:        []sql.Row{{"alpha"}, {"beta"}, {"gamma"}, {"prefix/one"}, {"zulu"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name = (select name from wanted where name = 'gamma')",
				Expected:        []sql.Row{{"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name in (select name from wanted) order by name",
				Expected:        []sql.Row{{"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select /*+ LOOKUP_JOIN(w, r) */ r.name from wanted w join dolt_branches r on w.name = r.name order by r.name",
				Expected:        []sql.Row{{"alpha"}, {"alpha"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select /*+ LOOKUP_JOIN(w, r) */ w.name, r.name from wanted w left join dolt_branches r on w.name = r.name order by w.name",
				Expected:        []sql.Row{{nil, nil}, {"alpha", "alpha"}, {"alpha", "alpha"}, {"gamma", "gamma"}, {"missing", nil}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:    "select name from dolt_branches where name is null",
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches where name <=> NULL",
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches where name not in ('alpha',NULL)",
				Expected: []sql.Row{},
			},
			{
				Query:           "select name from dolt_branches order by name desc limit 3",
				Expected:        []sql.Row{{"zulu"}, {"prefix/one"}, {"gamma"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name in ('alpha','gamma','beta') order by name desc",
				Expected:        []sql.Row{{"gamma"}, {"beta"}, {"alpha"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name from dolt_branches where name > 'alpha' and name < 'gamma' order by name",
				Expected:        []sql.Row{{"beta"}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query:           "select name,hash=hashof('alpha') from dolt_branches where name='alpha'",
				Expected:        []sql.Row{{"alpha", true}},
				ExpectedIndexes: []string{"dolt_branches_name_idx"},
			},
			{
				Query: "explain format=tree select name from dolt_branches where name='alpha'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_branches.name]"},
					{" └─ IndexedTableAccess(dolt_branches)"},
					{"     ├─ index: [dolt_branches.name]"},
					{"     └─ filters: [{[alpha, alpha]}]"},
				},
			},
			{
				Query: "explain format=tree select name from dolt_branches where name between 'alpha' and 'gamma' order by name desc",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_branches.name]"},
					{" └─ IndexedTableAccess(dolt_branches)"},
					{"     ├─ index: [dolt_branches.name]"},
					{"     ├─ filters: [{[alpha, gamma]}]"},
					{"     └─ reverse: true"},
				},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_branch('new_ref')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{{"new_ref"}},
			},
			{
				Query:    "call dolt_branch('-d','new_ref')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "execute ref_lookup",
				Expected: []sql.Row{},
			},
		},
	}
	t.Run("queries", func(t *testing.T) {
		h := newDoltEnginetestHarness(t)
		defer h.Close()
		enginetest.TestScript(t, h, script)
	})
	t.Run("prepared", func(t *testing.T) {
		h := newDoltEnginetestHarness(t)
		defer h.Close()
		enginetest.TestScriptPrepared(t, h, script)
	})
}
