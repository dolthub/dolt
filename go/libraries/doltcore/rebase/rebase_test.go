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

	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/store/types"
)


var initPeople = `insert into people 
  (id, first_name, last_name, is_married, age, rating) values
  (7, "Maggie", "Simpson", false, 1, 5.1),
  (8, "Milhouse", "Van Houten", false, 8, 3.5),
  (9, "Jacqueline", "Bouvier", true, 80, 2);`

var patty = `insert into people 
  (id, first_name, last_name, is_married, age, rating, drip) values
  (11, "Selma", "Bouvier", false, 40, 7, 8);`

var selma = `insert into people 
  (id, first_name, last_name, is_married, age, rating, drip) values
  (10, "Patty", "Bouvier", false, 40, 7, 8);`


func newPeopleRow(id int, first, last string, isMarried bool, age int, rating float64) row.Row {
	vals := row.TaggedValues{
		IdTag:        types.Int(id),
		FirstNameTag: types.String(first),
		LastNameTag:  types.String(last),
		IsMarriedTag: types.Bool(isMarried),
		AgeTag:       types.Int(age),
		RatingTag:    types.Float(rating),
	}

	r, err := row.New(types.Format_7_18, schema.SchemaFromCols(PeopleColColl()), vals)

	if err != nil {
		panic(err)
	}

	return r
}

func newPeopleRowWithDrip(id int, first, last string, isMarried bool, age int, rating float64, drip int) row.Row {
	vals := row.TaggedValues{
		IdTag:        types.Int(id),
		FirstNameTag: types.String(first),
		LastNameTag:  types.String(last),
		IsMarriedTag: types.Bool(isMarried),
		AgeTag:       types.Int(age),
		RatingTag:    types.Float(rating),
		postTag:	  types.Int(drip),
	}

	pcc, _ := PeopleColColl().Append(schema.NewColumn("drip", postTag, types.IntKind, false))
	r, err := row.New(types.Format_7_18, schema.SchemaFromCols(pcc), vals)

	if err != nil {
		panic(err)
	}

	return r
}

var PeopleRows = []row.Row{
	newPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
	newPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
	newPeopleRow(9, "Jacqueline", "Bouvier", true, 80, 2),
	newPeopleRowWithDrip(10, "Patty", "Bouvier", false, 40, 7, 8),
	newPeopleRowWithDrip(11, "Selma", "Bouvier", false, 40, 7, 8),
}

const (
	IdTag = iota
	FirstNameTag
	LastNameTag
	IsMarriedTag
	AgeTag
	emptyTag
	RatingTag
)

func PeopleColColl() *schema.ColCollection {
	pcc, _ := schema.NewColCollection(
		schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", AgeTag, types.IntKind, false),
		//		schema.NewColumn("empty", emptyTag, types.IntKind, false),
		schema.NewColumn("rating", RatingTag, types.FloatKind, false),
	)
	return pcc
}

func setup(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	peopleSch := schema.SchemaFromCols(PeopleColColl())
	dtestutils.CreateTestTable(t, dEnv, "people", peopleSch)
	executeQuery(dEnv, initPeople)
	_ = actions.StageAllTables(context.Background(), dEnv, false)
	_ = actions.CommitStaged(context.Background(), dEnv, "ship it", time.Now(), false)
	return dEnv
}

func executeQuery(dEnv *env.DoltEnv, query string) {
	root, _ := dEnv.WorkingRoot(context.Background())
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	_ = engine.Init()
	sqlCtx := sql.NewContext(context.Background())
	_, _, _ = engine.Query(sqlCtx, query)
	_ = dEnv.UpdateWorkingRoot(context.Background(), sqlDb.Root())
}

func checkTags(t *testing.T, r *doltdb.RootValue, tableName string, in []uint64, out []uint64) {
	tbl, _, err := r.GetTable(context.Background(), tableName)
	require.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)

	for _, tag := range out {
		_, found := sch.GetAllCols().GetByTag(tag)
		assert.False(t, found)
	}

	for _, tag := range in {
		_, found := sch.GetAllCols().GetByTag(tag)
		assert.True(t, found)
	}
}

func checkRows(t *testing.T, root *doltdb.RootValue, sch schema.Schema, query string, rows []row.Row) {
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	_ = engine.Init()
	sqlCtx := sql.NewContext(context.Background())

	s, rowIter, err := engine.Query(sqlCtx, query)
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
		assert.True(t, idx < len(rows))
		assert.Equal(t, rows[idx], rr)
		idx++
	}
}

const preTag = 13
const postTag = 19
func TestRebaseTag(t *testing.T) {
	dEnv := setup(t)

	var err error
	executeQuery(dEnv, fmt.Sprintf("alter table people add drip bigint comment 'tag:%d';", preTag))
	err = actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)
	err = actions.CommitStaged(context.Background(), dEnv, "ship it", time.Now(), false)
	require.NoError(t, err)

	executeQuery(dEnv, selma)
	executeQuery(dEnv, patty)
	_ = actions.StageAllTables(context.Background(), dEnv, false)
	_ = actions.CommitStaged(context.Background(), dEnv, "ship it", time.Now(), false)

	root, err := dEnv.WorkingRoot(context.Background())
	//p, _, _ := root.GetTable(context.Background(), "people")
	//fmt.Println(p.HashOf())
	require.NoError(t, err)
	checkTags(t, root, "people", []uint64{preTag}, []uint64{postTag})

	bs, _ := dEnv.DoltDB.GetBranches(context.Background())
	cm, err := RebaseSwapTag(context.Background(), bs[0], dEnv.DoltDB, preTag, postTag)
	require.NoError(t, err)

	root, _ = cm.GetRootValue()
	require.NoError(t, err)
	checkTags(t, root, "people", []uint64{postTag}, []uint64{preTag})
	pcc, _ := PeopleColColl().Append(schema.NewColumn("drip", postTag, types.IntKind, false))
	checkRows(t, root, schema.SchemaFromCols(pcc), "select * from people;", PeopleRows)

}

