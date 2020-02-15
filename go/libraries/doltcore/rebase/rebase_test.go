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
	_
	AgeTag
)
const DripTag = 13
const DripTagRebased = 19

type RebaseTagTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	Commands []command
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

type command interface {
	exec(t *testing.T, dEnv *env.DoltEnv)
}

type commit struct {
	message string
}

func (c commit) exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)
	err = actions.CommitStaged(context.Background(), dEnv, c.message, time.Now(), false)
	require.NoError(t, err)
	cm, _ := commands.ResolveCommitWithVErr(dEnv, "HEAD", dEnv.RepoState.Head.Ref.String())
	ch, _ := cm.HashOf()
	fmt.Println(fmt.Sprintf("commit: %s", ch.String()))
}

type query struct {
	query string
}

func (q query) exec(t *testing.T, dEnv *env.DoltEnv) {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	err = engine.Init()
	require.NoError(t, err)
	sqlCtx := sql.NewContext(context.Background())
	_, _, err = engine.Query(sqlCtx, q.query)
	require.NoError(t, err)
	err = dEnv.UpdateWorkingRoot(context.Background(), sqlDb.Root())
	require.NoError(t, err)
}

type branch struct {
	branchName string
}

func (b branch) exec(t *testing.T, dEnv *env.DoltEnv) {
	cwb := dEnv.RepoState.Head.Ref.String()
	err := actions.CreateBranch(context.Background(), dEnv, b.branchName, cwb, false)
	require.NoError(t, err)
}

type checkout struct {
	branchName string
}

func (c checkout) exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.CheckoutBranch(context.Background(), dEnv, c.branchName)
	require.NoError(t, err)
}

type merge struct {
	branchName string
}

func (m merge) exec(t *testing.T, dEnv *env.DoltEnv) {
	mm := commands.MergeCmd{}
	status := mm.Exec(context.Background(), "dolt merge", []string{m.branchName}, dEnv)
	assert.Equal(t, 0, status)
}

var createPeopleTable = fmt.Sprintf(`
	create table people (
		id bigint comment 'tag:%d',
		name varchar(20) not null comment 'tag:%d',
		age bigint comment 'tag:%d',
		primary key (id)
	);`, IdTag, NameTag, AgeTag)

func columnCollection(cols ...schema.Column) *schema.ColCollection {
	pcc, _ := schema.NewColCollection(cols...)
	return pcc
}

func newRow(vals row.TaggedValues, cc *schema.ColCollection) row.Row {
	r, _ := row.New(types.Format_7_18, schema.SchemaFromCols(cc), vals)
	return r
}

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
		Commands: []command{
			query{createPeopleTable},
			commit{"made changes"},
		},
		OldTag:         DripTag,
		NewTag:         DripTagRebased,
		ExpectedErrStr: "tag: " + strconv.Itoa(DripTag) + " not found in commit history for commit: ",
	},
	{
		Name: "rebase entire history",
		Commands: []command{
			query{createPeopleTable},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values
				(10, "Patty Bouvier", 40, 8.5),
				(11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			commit{"made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows:      []row.Row{},
	},
	{
		Name: "create new column, insert value to column, rebase column's tag",
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`update people set drip=9.9 where id=9;`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			query{`update people set drip=9.9 where id=11;`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			query{`update people set drip=9.9 where id=11;`},
			query{`update people set drip=1.1 where id=9;`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values 
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			commit{"made changes"},
			query{`update people set age=2 where id=7;`},
			query{`delete from people where id=8;`},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			commit{"made changes"},
			branch{"newBranch"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			checkout{"newBranch"},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			checkout{"master"},
			merge{"newBranch"},
			commit{"made changes"},
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
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values
				(6, "Homer Simpson", 44),
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8),
				(9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			branch{"newBranch"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`delete from people where id=6;`},
			query{`update people set drip=99.9 where id=7;`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			checkout{"newBranch"},
			query{`insert into people (id, name, age) values (10, "Patty Bouvier", 40);`},
			query{`delete from people where id=8;`},
			query{`update people set age=40 where id=9;`},
			commit{"made changes"},
			checkout{"master"},
			merge{"newBranch"},
			commit{"made changes"},
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
		Name: "create new column on other branch, merge into master",
		Commands: []command{
			query{createPeopleTable},
			commit{"made changes"},
			branch{"newBranch"},
			checkout{"newBranch"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			checkout{"master"},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			merge{"newBranch"},
			commit{"made changes"},
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
		Name: "create new column, use on multiple branches, merge",
		Commands: []command{
			query{createPeopleTable},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			query{`insert into people (id, name, age, drip) values (9, "Jacqueline Bouvier", 80, 8.5);`},
			commit{"made changes"},
			branch{"newBranch"},
			checkout{"newBranch"},
			query{`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			commit{"made changes"},
			checkout{"master"},
			query{`insert into people (id, name, age, drip) values (10, "Patty Bouvier", 40, 8.5);`},
			commit{"made changes"},
			merge{"newBranch"},
			commit{"made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "rebase tag that exists in history but not at the head commit",
		Commands: []command{
			query{createPeopleTable},
			query{`insert into people (id, name, age) values 
				(7, "Maggie Simpson", 1);`},
			commit{"made changes"},
			query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
			commit{"made changes"},
			query{`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			commit{"made changes"},
			query{`delete from people where id=9;`},
			commit{"made changes"},
			query{`alter table people drop column drip;`},
			commit{"made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(people),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
		},
	},
}

func TestRebaseTag(t *testing.T) {
	for _, test := range RebaseTagTests {
		t.Run(test.Name, func(t *testing.T) {
			testRebaseTag(t, test)
		})
	}
	t.Run("ensure that rebased history does not overlap with previous history", testRebaseTagHistory)
}

func testRebaseTag(t *testing.T, test RebaseTagTest) {
	validateTest(t, test)

	dEnv := dtestutils.CreateTestEnv()
	for _, cmd := range test.Commands {
		cmd.exec(t, dEnv)
	}

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	rebasedCommit, err := TagRebase(context.Background(), bs[0], dEnv.DoltDB, test.OldTag, test.NewTag)

	if test.ExpectedErrStr != "" {
		require.NotNil(t, err)
		require.Contains(t, err.Error(), test.ExpectedErrStr)
	} else {
		require.NoError(t, err)
		require.NotNil(t, rebasedCommit)

		mcs, _ := doltdb.NewCommitSpec("HEAD", "master")
		masterCm, _ := dEnv.DoltDB.Resolve(context.Background(), mcs)
		rch, _ := rebasedCommit.HashOf()
		mch, _ := masterCm.HashOf()
		require.Equal(t, rch, mch)

		rebasedRoot, _ := rebasedCommit.GetRootValue()
		checkSchema(t, rebasedRoot, "people", test.ExpectedSchema)
		checkRows(t, rebasedRoot, test.ExpectedSchema, test.SelectResultQuery, test.ExpectedRows)
	}
}

func testRebaseTagHistory(t *testing.T) {
	cmds := []command{
		query{createPeopleTable},
		query{`insert into people (id, name, age) values 
				(7, "Maggie Simpson", 1);`},
		commit{"made changes"}, // common ancestor of (newMaster, oldMaster) and (newMaster, other)

		query{`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`},
		commit{"made changes"}, // common ancestor of (oldMaster, other)

		branch{"other"},
		query{`insert into people (id, name, age, drip) values (10, "Patty Bouvier", 40, 8.5);`},
		commit{"made changes"},
	}

	dEnv := dtestutils.CreateTestEnv()
	for _, cmd := range cmds {
		cmd.exec(t, dEnv)
	}

	mcs, _ := doltdb.NewCommitSpec("HEAD", "master")
	oldMasterCm, _ := dEnv.DoltDB.Resolve(context.Background(), mcs)
	ocs, _ := doltdb.NewCommitSpec("HEAD", "other")
	otherCm, _ := dEnv.DoltDB.Resolve(context.Background(), ocs)

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	newMasterCm, err := TagRebase(context.Background(), bs[0], dEnv.DoltDB, DripTag, DripTagRebased)
	require.NoError(t, err)

	expectedSch := schema.SchemaFromCols(peopleWithDrip)
	rebasedRoot, _ := newMasterCm.GetRootValue()
	checkSchema(t, rebasedRoot, "people", expectedSch)
	checkRows(t, rebasedRoot, expectedSch, "select * from people;", []row.Row{
		newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
		newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	})

	// assert that histories have been forked
	anc1, err := doltdb.GetCommitAncestor(context.Background(), oldMasterCm, otherCm)
	require.NoError(t, err)
	ancHash1, _ := anc1.HashOf()

	anc2, err := doltdb.GetCommitAncestor(context.Background(), newMasterCm, oldMasterCm)
	require.NoError(t, err)
	ancHash2, _ := anc2.HashOf()

	anc3, err := doltdb.GetCommitAncestor(context.Background(), newMasterCm, otherCm)
	require.NoError(t, err)
	ancHash3, _ := anc3.HashOf()

	require.NotEqual(t, ancHash1, ancHash2)
	require.NotEqual(t, ancHash1, ancHash3)
	require.Equal(t, ancHash2, ancHash3)
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

func checkSchema(t *testing.T, r *doltdb.RootValue, tableName string, expectedSch schema.Schema) {
	tbl, _, err := r.GetTable(context.Background(), tableName)
	require.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)
	eq, err := schema.SchemasAreEqual(sch, expectedSch)
	require.NoError(t, err)
	require.True(t, eq)
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
		require.True(t, idx < len(expectedRows))
		require.Equal(t, expectedRows[idx], actualRows[idx])
		idx++
	}
}
