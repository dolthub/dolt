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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	dtu "github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	tc "github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

type MigrateTagsTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	Commands []tc.Command
}

var MigrateTagsTests = []MigrateTagsTest{
	{
		Name: "Can migrate tags",
		Commands: []tc.Command{
			putTable{TableName: "tableOne", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			putTable{TableName: "tableTwo", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "created two tables"},
		},
	},
	{
		Name: "Can migrate multiple branches",
		Commands: []tc.Command{
			tc.Branch{BranchName: "init"},
			putTable{TableName: "tableOne", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "created tableOne on master"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			putTable{TableName: "tableTwo", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "create tableTwo on other"},
		},
	},
	{
		Name: "Can migrate branches with merges",
		Commands: []tc.Command{
			tc.Branch{BranchName: "init"},
			putTable{TableName: "tableOne", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "created tableOne on master"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			putTable{TableName: "tableTwo", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "create tableTwo on other"},
			tc.Checkout{BranchName: "master"},
			putTable{TableName: "tableThree", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "created tableThree on other"},
			tc.Merge{BranchName: "other"},
		},
	},
	{
		Name: "Can migrate dropped tables",
		Commands: []tc.Command{
			tc.Branch{BranchName: "init"},
			putTable{TableName: "tableOne", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "created tableOne on master"},
			putTable{TableName: "tableTwo", Schema: schema.SchemaFromCols(columnCollection(
				newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
				newColTypeInfo("c0", 1, varchar(20), false)))},
			tc.CommitAll{Message: "create tableTwo on master"},
			tc.Query{Query: "drop table tableTwo;"},
			tc.CommitAll{Message: "dropped tableTwo on master"},
		},
	},
	//{
	//	Name: "Docs test",
	//	Commands: []tc.Command{
	//		tc.Query{Query: "create table t (pk int not null primary key);"},
	//		tc.CommitAll{Message: "Committing initial docs"},
	//		tc.Branch{BranchName: "test-a"},
	//		tc.Branch{BranchName: "test-b"},
	//		tc.Checkout{BranchName: "test-a"},
	//		putDoc{DocName: doltdb.ReadmePk, DocText: "test-a"},
	//		tc.CommitAll{Message: "Changed README.md on test-a branch"},
	//		tc.Checkout{BranchName: "test-b"},
	//		putDoc{DocName: doltdb.ReadmePk, DocText: "test-b"},
	//		tc.CommitAll{Message: "Changed README.md on test-b branch"},
	//		tc.Checkout{BranchName: "master"},
	//		tc.Merge{BranchName: "test-a"},
	//		tc.Merge{BranchName: "test-b"},
	//		tc.ConflictsCat{TableName: doltdb.DocTableName },
	//	},
	//},
}

// putTable allows us to create tables with conflicting tags
type putTable struct {
	TableName string
	Schema 	  schema.Schema
}

// CommandString returns "query".
func (p putTable) CommandString() string { return "put_table" }

// Exec executes a Query command on a test dolt environment.
func (p putTable) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	newRoot, err := root.CreateEmptyTable(context.Background(), p.TableName, p.Schema)
	if err != nil {
		return err
	}
	return dEnv.UpdateWorkingRoot(context.Background(), newRoot)
}

// putTable allows us to create tables with conflicting tags
type putDoc struct {
	DocName string
	DocText string
}

// CommandString returns "query".
func (p putDoc) CommandString() string { return "put_doc" }

// Exec executes a Query command on a test dolt environment.
func (p putDoc) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)

	schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), root.VRW(), env.DoltDocsSchema)
	require.NoError(t, err)

	m, err := types.NewMap(context.Background(), root.VRW())
	require.NoError(t, err)

	me := m.Edit()
	me.Set(
		row.TaggedValues{schema.DocNameTag: types.String(p.DocName)}.NomsTupleForTags(root.VRW().Format(), []uint64{schema.DocNameTag}, true),
		row.TaggedValues{schema.DocTextTag: types.String(p.DocText)}.NomsTupleForTags(root.VRW().Format(), []uint64{schema.DocTextTag}, false))
	newMap, err := me.Map(context.Background())
	require.NoError(t, err)

	tbl, err := doltdb.NewTable(context.Background(), root.VRW(), schVal, newMap)

	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(context.Background(), doltdb.DocTableName, tbl)

	if err != nil {
		return err
	}

	h, _ := root.HashOf()
	nh, _ := newRoot.HashOf()
	require.NotEqual(t, h, nh)

	return dEnv.UpdateWorkingRoot(context.Background(), newRoot)
}

func TestMigrateUniqueTags(t *testing.T) {
	for _, test := range MigrateTagsTests {
		t.Run(test.Name, func(t *testing.T) {
			testMigrateUniqueTags(t, test)
		})
	}
}

func testMigrateUniqueTags(t *testing.T, test MigrateTagsTest) {
	dEnv := dtu.CreateTestEnv()
	for idx, cmd := range test.Commands {
		fmt.Println(fmt.Sprintf("%d: %s", idx, cmd.CommandString()))
		err := cmd.Exec(t, dEnv)
		require.NoError(t, err)
	}

	cwb := dEnv.RepoState.CWBHeadRef().String()
	bb, err := dEnv.DoltDB.GetBranches(context.Background())
	require.NoError(t, err)

	// rebase the history to uniquify tags
	err = migrateUniqueTags(context.Background(), nil, dEnv.DoltDB, bb)
	require.NoError(t, err)

	// confirm that the repo state is the same
	assert.Equal(t, cwb, dEnv.RepoState.CWBHeadRef().String())

	// confirm that tags are unique
	for _, b := range bb {

		cs, err := doltdb.NewCommitSpec("head", b.String())
		require.NoError(t, err)

		c, err := dEnv.DoltDB.Resolve(context.Background(), cs)
		require.NoError(t, err)

		r, err := c.GetRootValue()
		require.NoError(t, err)

		allTags := make(map[uint64]struct{})
		tblNames, err := r.GetTableNames(context.Background())
		require.NoError(t, err)

		for _, tn := range tblNames {
			tbl, _, err := r.GetTable(context.Background(), tn)
			require.NoError(t, err)

			sch, err := tbl.GetSchema(context.Background())
			require.NoError(t, err)

			for _, tag := range sch.GetAllCols().Tags {
				_, found := allTags[tag]
				require.False(t, found)
				allTags[tag] = struct{}{}
			}
		}
	}
}