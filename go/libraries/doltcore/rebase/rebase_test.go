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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	stu "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"

)


var initPeople = `insert into people 
  (id, first_name, last_name, is_married, age, rating) values
  (7, "Maggie", "Simpson", false, 1, 5.1),
  (8, "Milhouse", "Van Houten", false, 8, 3.5),
  (9, "Jacqueline", "Bouvier", true, 80, 2);`

var patty = `insert into people 
  (id, first_name, last_name, is_married, age, rating, drip) values
  (11, "Selma", "Bouvier", false, 40, 7, 8.5);`

var selma = `insert into people 
  (id, first_name, last_name, is_married, age, rating, drip) values
  (10, "Patty", "Bouvier", false, 40, 7, 8.5);`

var rows = []row.Row{
	stu.NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
	stu.NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
	stu.NewPeopleRow(9, "Jacqueline", "Bouvier", true, 80, 2),
	stu.NewPeopleRow(10, "Patty", "Bouvier", false, 40, 7),
	stu.NewPeopleRow(11, "Selma", "Bouvier", false, 40, 7),
}


func setup(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	stu.CreateEmptyTestDatabase(dEnv, t)
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

func TestRebaseTag(t *testing.T) {
	dEnv := setup(t)

	executeQuery(dEnv, "alter table people add drip bigint comment 'tag:13';")
	_ = actions.StageAllTables(context.Background(), dEnv, false)
	_ = actions.CommitStaged(context.Background(), dEnv, "ship it", time.Now(), false)

	executeQuery(dEnv, selma)
	executeQuery(dEnv, patty)
	_ = actions.StageAllTables(context.Background(), dEnv, false)
	_ = actions.CommitStaged(context.Background(), dEnv, "ship it", time.Now(), false)

	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	checkTags(t, root, "people", []uint64{13}, []uint64{19})

	bs, _ := dEnv.DoltDB.GetBranches(context.Background())
	cm, err := RebaseSwapTag(context.Background(), bs[0], dEnv.DoltDB, 13, 19)
	require.NoError(t, err)

	root, _ = cm.GetRootValue()
	require.NoError(t, err)
	checkTags(t, root, "people", []uint64{19}, []uint64{13})
}

