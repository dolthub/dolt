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

package envtestutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	tc "github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
)

type UniqueTagsTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	Commands []tc.Command
	// Name of the table to be verified
	TableName string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// Expected branch
	ExpectedBranch string
	// An expected error string
	ExpectedErrStr string
}

var UniqueTagsTests = []UniqueTagsTest{
	{
		Name: "can create table with tags specified",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:42');`},
		},
		TableName: "test",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", 42, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedBranch: "master",
	},
	{
		Name: "cannot create duplicate tags within a table",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (
				pk int not null primary key comment 'tag:42',
				c0 int comment 'tag:42');`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot create duplicate tags across tables",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:42');`},
			tc.Query{Query: `create table test2 (pk int not null primary key comment 'tag:42');`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot add a duplicate tag within a table",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:42');`},
			tc.Query{Query: `alter table test add column c0 int comment 'tag:42';`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot add a duplicate tag across tables",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:0');`},
			tc.Query{Query: `create table other (pk int not null primary key comment 'tag:42');`},
			tc.Query{Query: `alter table test add column c0 int comment 'tag:42';`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot add a tag that has previously existed in the same table's history",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (
				pk int not null primary key comment 'tag:0',
				c0 int comment 'tag:42');`},
			tc.CommitAll{Message: "created table test"},
			tc.Query{Query: `alter table test drop column c0;`},
			tc.CommitAll{Message: "dropped c0"},
			tc.Query{Query: `alter table test add column c1 int comment 'tag:42';`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot add a tag that has previously existed in a different table's history",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:0');`},
			tc.Query{Query: `create table other (
				pk int not null primary key comment 'tag:1',
				c0 int comment 'tag:42');`},
			tc.CommitAll{Message: "created tables test and other"},
			tc.Query{Query: `alter table other drop column c0;`},
			tc.CommitAll{Message: "dropped c0 from other"},
			tc.Query{Query: `alter table test add column c1 int comment 'tag:42';`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "cannot add a tag that has previously existed in a merged branch's history",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:0');`},
			tc.CommitAll{Message: "created table test"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: `alter table test add column c0 int comment 'tag:42';`},
			tc.CommitAll{Message: "added column c0 to test on branch other"},
			tc.Query{Query: `alter table test drop column c0;`},
			tc.CommitAll{Message: "dropped c0 from test on other"},
			tc.Checkout{BranchName: "master"},
			tc.Merge{BranchName: "other"},
			tc.Query{Query: `alter table test add column c1 int comment 'tag:42';`},
		},
		ExpectedBranch: "master",
		ExpectedErrStr: "two different columns with the same tag",
	},
	{
		Name: "tag conflicts will be rebased on merge",
		Commands: []tc.Command{
			tc.Query{Query: `create table test (pk int not null primary key comment 'tag:0');`},
			tc.CommitAll{Message: "created table test"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: `alter table test add column c1 int comment 'tag:42';`},
			tc.CommitAll{Message: "added column c1 with tag 42 to test on branch other"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: `alter table test add column c0 int comment 'tag:42';`},
			tc.CommitAll{Message: "added column c0 with tag 42 to test on branch master"},
			tc.Merge{BranchName: "other"},
			//tc.Query{Query: `alter table test add column c1 int comment 'tag:42';`},
		},
		TableName: "test",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", 0, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", 42, typeinfo.Int32Type, false),
			newColTypeInfo("c1", 10707, typeinfo.Int32Type, false),
		)),
		ExpectedBranch: "master",
	},
	{
		Name: "tag conflicts in multiple tables will be rebased on merge",
		Commands: []tc.Command{
			tc.Query{Query: `create table one (pk1 int not null primary key comment 'tag:1');`},
			tc.Query{Query: `create table two (pk2 int not null primary key comment 'tag:2');`},
			tc.CommitAll{Message: "created two tables on master"},
			tc.Branch{BranchName: "myBranch"},
			tc.Query{Query: `alter table one add column c1 float comment 'tag:142';`},
			tc.Query{Query: `alter table two add column c2 float comment 'tag:242';`},
			tc.CommitAll{Message: "added columns to both tables on master"},
			tc.Checkout{BranchName: "myBranch"},
			tc.Query{Query: `alter table one add column c11 float comment 'tag:142';`},
			tc.Query{Query: `alter table two add column c22 float comment 'tag:242';`},
			tc.CommitAll{Message: "added columns to both tables on myBranch"},
			tc.Checkout{BranchName: "master"},
			tc.Merge{BranchName: "myBranch"},
		},
		TableName: "two",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk2", 2, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c2", 242, typeinfo.Float32Type, false),
			newColTypeInfo("c22", 310, typeinfo.Float32Type, false),
		)),
		ExpectedBranch: "master",
	},
}

func TestUniqueTags(t *testing.T) {
	for _, test := range UniqueTagsTests {
		t.Run(test.Name, func(t *testing.T) {
			testUniqueTags(t, test)
		})
	}
}

func testUniqueTags(t *testing.T, test UniqueTagsTest) {
	dEnv := dtestutils.CreateTestEnv()

	var ee error
	for idx, cmd := range test.Commands {
		require.NoError(t, ee)
		fmt.Println(fmt.Sprintf("%d: %s: %s", idx, cmd.CommandString(), cmd))
		ee = cmd.Exec(t, dEnv)
	}

	if test.ExpectedErrStr != "" {
		require.Error(t, ee, test.ExpectedErrStr)
	} else {
		spec := dEnv.RepoState.CWBHeadRef()
		require.Equal(t, "refs/heads/"+test.ExpectedBranch, spec.String())

		r, err := dEnv.WorkingRoot(context.Background())
		require.NoError(t, err)

		tbl, ok, err := r.GetTable(context.Background(), test.TableName)
		require.NoError(t, err)
		require.True(t, ok)

		sch, err := tbl.GetSchema(context.Background())
		require.NoError(t, err)
		assert.Equal(t, test.ExpectedSchema, sch)
	}
}
