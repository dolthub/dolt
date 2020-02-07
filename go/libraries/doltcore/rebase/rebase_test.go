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

	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
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

const commit = "commit"

type RebaseTagTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	ModifyingQueries []string
	// The pairs {old, new} of tags that need to be exchanged
	TagMap map[uint64]uint64
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
	newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(8), NameTag: types.String("Milhouse Van Houten"), AgeTag: types.Int(8)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
}

var RebaseTagTests = []RebaseTagTest{
	{
		Name: "rebase non-existent tag",
		ModifyingQueries: []string{
			createPeopleTable,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		ExpectedErrStr: "not found in any table at commit:",
	},
	{
		Name: "create new column, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, update column value in existing row, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			`update people set drip=9.9 where id=9;`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			`update people set drip=9.9 where id=11;`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			`insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`,
			commit,
			`update people set drip=9.9 where id=11;`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, modify rows without value for column, rebase column's tag",
		ModifyingQueries: []string{
			createPeopleTable,
			`insert into people (id, name, age) values 
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8);`,
			commit,
			`alter table people add drip float comment 'tag:` + strconv.Itoa(DripTag) + `';`,
			commit,
			`update people set age=2 where id=7;`,
			`delete from people where id=8`,
			`insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`,
			commit,
		},
		TagMap:            map[uint64]uint64{DripTag: DripTagRebased},
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.SchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(2)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
		},
	},
}

func TestRebaseTag(t *testing.T) {
	for _, test := range RebaseTagTests {
		t.Run(test.Name, func(t *testing.T) {
			testRebaseTag(t, test)
		})
	}
}

func testRebaseTag(t *testing.T, test RebaseTagTest) {
	dEnv := dtestutils.CreateTestEnv()

	for _, q := range test.ModifyingQueries {
		if q == commit {
			commitAll(dEnv, "made changes")
		} else {
			executeQuery(t, dEnv, q)
		}
	}

	root, _ := dEnv.WorkingRoot(context.Background())

	var oldTag, newTag uint64
	for oldTag, newTag = range test.TagMap {
		// TODO: update this once we have tagMap in rebase
		break
	}

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	rebasedCommit, err := RebaseTag(context.Background(), bs[0], dEnv.DoltDB, oldTag, newTag)

	if test.ExpectedErrStr != "" {
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), test.ExpectedErrStr)
	} else {
		require.NoError(t, err)

		// verify the pre-rebase tags
		checkTags(t, root, "people", reverseTags(test.TagMap))

		rebasedRoot, _ := rebasedCommit.GetRootValue()
		checkTags(t, rebasedRoot, "people", test.TagMap)
		checkRows(t, rebasedRoot, test.ExpectedSchema, test.SelectResultQuery, test.ExpectedRows)
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

func commitAll(dEnv *env.DoltEnv, msg string) {
	_ = actions.StageAllTables(context.Background(), dEnv, false)
	_ = actions.CommitStaged(context.Background(), dEnv, msg, time.Now(), false)
}

func reverseTags(m map[uint64]uint64) map[uint64]uint64 {
	newMap := make(map[uint64]uint64)
	for k, v := range m {
		newMap[v] = k
	}
	return newMap
}

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
	_, _ = dsqle.SqlSchemaToDoltSchema(s)
	require.NoError(t, err)

	var r sql.Row
	var rr row.Row
	idx := 0
	for err == nil {
		r, err = rowIter.Next()
		if err == io.EOF {
			return
		}
		require.NoError(t, err)
		rr, err = dsqle.SqlRowToDoltRow(root.VRW().Format(), r, sch)
		require.NoError(t, err)
		assert.True(t, idx < len(expectedRows))
		assert.Equal(t, expectedRows[idx], rr)
		idx++
	}
}
