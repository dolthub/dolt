// Copyright 2020 Liquidata, Inc.
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

package rebase

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	IdTag = iota
	NameTag
	emptyTag
	AgeTag
	MarriedTag
)
const DripTag = 13
const DripTagRebased = 19

const query = "query"
const commit = "commit"
const branch = "branch"
const checkout = "checkout"
const merge = "merge"

type RebaseTagTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	Commands []string
	// The pairs {old, new} of tags that need to be exchanged
	OldTag uint64
	NewTag uint64
	// The select query to run to verify the results
	SelectResultQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows the select query should return, nil if an error is expected
	ExpectedRows []row.Row
	// An expected error string
	ExpectedErrStr string
}

func columnCollection(cols ...schema.Column) *schema.ColCollection {
	pcc, _ := schema.NewColCollection(cols...)
	return pcc
}

func newRow(vals row.TaggedValues, cc *schema.ColCollection) row.Row {
	r, _ := row.New(types.Format_7_18, schema.SchemaFromCols(cc), vals)
	return r
}

var createPeopleTable = fmt.Sprintf(`
	create table people (
		id bigint comment 'tag:%d',
		name varchar(20) comment 'tag:%d',
		age bigint comment 'tag:%d',
		primary key (id)
	);`, IdTag, NameTag, AgeTag)

var people = columnCollection(
	schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", NameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("age", AgeTag, types.IntKind, false),
)

var peopleWithDrip = columnCollection(
	schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("name", NameTag, types.StringKind, false, schema.NotNullConstraint{}),
	schema.NewColumn("age", AgeTag, types.IntKind, false),
	schema.NewColumn("drip", DripTagRebased, types.FloatKind, false),
)

var peopleRows = []row.Row{
	newRow(row.TaggedValues{IdTag: types.Int(6), NameTag: types.String("Homer Simpson"), AgeTag: types.Int(44)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(8), NameTag: types.String("Milhouse Van Houten"), AgeTag: types.Int(8)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
}

var RebaseTagTests = []RebaseTagTest{
	{
		Name: "rebase non-existent tag",
		Commands: []string{
			query + createPeopleTable,
			commit,
		},
		OldTag:         DripTag,
		NewTag:         DripTagRebased,
		ExpectedErrStr: "not found in any table at commit:",
	},
	{
		Name: "rebase entire history",
		Commands: []string{
			query + createPeopleTable,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values
				(10, "Patty Bouvier", 40, 8.5),
				(11, "Selma Bouvier", 40, 8.5);`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows:      []row.Row{},
	},
	{
		Name: "create new column, insert value to column, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, update column value in existing row, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `update people set drip=9.9 where id=9;`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			query + `update people set drip=9.9 where id=11;`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			query + `update people set drip=9.9 where id=11;`,
			query + `update people set drip=1.1 where id=9;`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(1.1)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, modify rows without value for column, rebase column's tag",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values 
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8);`,
			commit,
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			commit,
			query + `update people set age=2 where id=7;`,
			query + `delete from people where id=8`,
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(2)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
		},
	},
	{
		Name: "create new column on master, insert to table on other branch, merge",
		Commands: []string{
			query + createPeopleTable,
			commit,
			branch + "newBranch",
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			checkout + "newBranch",
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			checkout + "master",
			merge + "newBranch",
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column on master; insert, update, delete on both branches; merge",
		Commands: []string{
			query + createPeopleTable,
			query + `insert into people (id, name, age) values
				(6, "Homer Simpson", 44),
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8),
				(9, "Jacqueline Bouvier", 80);`,
			commit,
			branch + "newBranch",
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `delete from people where id=6;`,
			query + `update people set drip=99.9 where id=7;`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			checkout + "newBranch",
			query + `insert into people (id, name, age) values (10, "Patty Bouvier", 40);`,
			query + `delete from people where id=8;`,
			query + `update people set age=40 where id=9;`,
			commit,
			checkout + "master",
			merge + "newBranch",
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1), DripTagRebased: types.Float(99.9)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(40)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column on other branch, merge into mater",
		Commands: []string{
			query + createPeopleTable,
			commit,
			branch + "newBranch",
			checkout + "newBranch",
			query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			checkout + "master",
			query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			merge + "newBranch",
			commit,
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	//{  TODO: make a bats test to confirm that you cannot merge into a dirty working set
	//	Name: "create new column on and merge in same commit",
	//	Commands: []string{
	//		query + createPeopleTable,
	//		commit,
	//		branch + "newBranch",
	//		checkout + "newBranch",
	//		query + `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
	//		commit,
	//		checkout + "master",
	//		query + `alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
	//		query + `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
	//		merge + "newBranch",
	//		commit,
	//	},
	//	OldTag:            DripTag,
	//	NewTag:            DripTagRebased,
	//	SelectResultQuery: "select * from people;",
	//	ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
	//	ExpectedRows: []row.Row{
	//		newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
	//		newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	//	},
	//},
}

func TestRebaseTag(t *testing.T) {
	for _, test := range RebaseTagTests {
		t.Run(test.Name, func(t *testing.T) {
			testRebaseTag(t, test)
		})
	}
}

func testRebaseTag(t *testing.T, test RebaseTagTest) {
	validateTest(t, test)

	dEnv := dtestutils.CreateTestEnv()
	for _, cmd := range test.Commands {
		switch {
		case cmd[:len(query)] == query:
			executeQuery(t, dEnv, cmd[len(query):])
		case cmd[:len(commit)] == commit:
			commitAll(t, dEnv, "made changes")
		case cmd[:len(branch)] == branch:
			createNewBranch(t, dEnv, cmd[len(branch):])
		case cmd[:len(checkout)] == checkout:
			checkoutBranch(t, dEnv, cmd[len(checkout):])
		case cmd[:len(merge)] == merge:
			mergeBranch(t, dEnv, cmd[len(merge):])
		}
	}

	root, _ := dEnv.WorkingRoot(context.Background())

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	rebasedCommit, err := RebaseTag(context.Background(), bs[0], dEnv.DoltDB, test.OldTag, test.NewTag)

	if test.ExpectedErrStr != "" {
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), test.ExpectedErrStr)
	} else {
		require.NoError(t, err)

		// verify the pre-rebase tags
		checkTags(t, root, "people", map[uint64]uint64{test.NewTag: test.OldTag})

		rebasedRoot, _ := rebasedCommit.GetRootValue()
		checkTags(t, rebasedRoot, "people", map[uint64]uint64{test.OldTag: test.NewTag})
		checkRows(t, rebasedRoot, test.ExpectedSchema, test.SelectResultQuery, test.ExpectedRows)
	}
}

func validateTest(t *testing.T, test RebaseTagTest) {
	require.NotNil(t, test.Name)
	require.NotNil(t, test.Commands)
	require.NotNil(t, test.OldTag)
	require.NotNil(t, test.NewTag)
	require.NotNil(t, test.SelectResultQuery)
	if test.ExpectedErrStr == "" {
		require.NotNil(t, test.ExpectedSchema)
		require.NotNil(t, test.ExpectedRows)
	}
}

func executeQuery(t *testing.T, dEnv *env.DoltEnv, query string) {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	err = engine.Init()
	require.NoError(t, err)
	sqlCtx := sql.NewContext(context.Background())
	_, _, err = engine.Query(sqlCtx, query)
	require.NoError(t, err)
	err = dEnv.UpdateWorkingRoot(context.Background(), sqlDb.Root())
	require.NoError(t, err)
}

func commitAll(t *testing.T, dEnv *env.DoltEnv, msg string) {
	err := actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)
	err = actions.CommitStaged(context.Background(), dEnv, msg, time.Now(), false)
	require.NoError(t, err)
	cm, _ := commands.ResolveCommitWithVErr(dEnv, "HEAD", dEnv.RepoState.Head.Ref.String())
	ch, _ := cm.HashOf()
	fmt.Println(fmt.Sprintf("commit: %s", ch.String()))
}

func createNewBranch(t *testing.T, dEnv *env.DoltEnv, branchName string) {
	cwb := dEnv.RepoState.Head.Ref.String()
	err := actions.CreateBranch(context.Background(), dEnv, branchName, cwb, false)
	require.NoError(t, err)
}

func checkoutBranch(t *testing.T, dEnv *env.DoltEnv, branchName string) {
	err := actions.CheckoutBranch(context.Background(), dEnv, branchName)
	require.NoError(t, err)
}

func mergeBranch(t *testing.T, dEnv *env.DoltEnv, branchName string) {
	m := commands.MergeCmd{}
	status := m.Exec(context.Background(), "dolt merge", []string{branchName}, dEnv)
	assert.Equal(t, 0, status)
}

//func reverseTags(m map[uint64]uint64) map[uint64]uint64 {
//	newMap := make(map[uint64]uint64)
//	for k, v := range m {
//		newMap[v] = k
//	}
//	return newMap
//}

func checkTags(t *testing.T, r *doltdb.RootValue, tableName string, tagMap map[uint64]uint64) {
	tbl, _, err := r.GetTable(context.Background(), tableName)
	require.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)

	require.True(t, len(tagMap) > 0)
	for absentTag, existingTag := range tagMap {
		_, found := sch.GetAllCols().GetByTag(absentTag)
		assert.False(t, found)
		_, found = sch.GetAllCols().GetByTag(existingTag)
		assert.True(t, found)
	}
}

func checkRows(t *testing.T, root *doltdb.RootValue, sch schema.Schema, selectQuery string, expectedRows []row.Row) {
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	_ = engine.Init()
	sqlCtx := sql.NewContext(context.Background())

	s, rowIter, err := engine.Query(sqlCtx, selectQuery)
	require.NoError(t, err)
	_, err = dsqle.SqlSchemaToDoltSchema(s)
	require.NoError(t, err)

	actualRows := []row.Row{}
	for {
		r, err := rowIter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rr, err := dsqle.SqlRowToDoltRow(root.VRW().Format(), r, sch)
		require.NoError(t, err)
		actualRows = append(actualRows, rr)
	}

	require.Equal(t, len(actualRows), len(expectedRows))

	for idx := 0; idx < len(expectedRows); idx++ {
		assert.True(t, idx < len(expectedRows))
		assert.Equal(t, expectedRows[idx], actualRows[idx])
		idx++
	}
}
