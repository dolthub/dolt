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

const (
	pkTag = iota
	c0Tag
	c1Tag
	c11Tag
	c12Tag
)

type SuperSchemaTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// Name of the table to be verified
	TableName string
	// The modifying queries to run
	Commands []tc.Command
	// Expected branch
	ExpectedBranch string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows the select query should return, nil if an error is expected
	ExpectedSuperSchema *schema.SuperSchema
	// An expected error string
	ExpectedErrStr string
}

var testableDef = fmt.Sprintf("create table testable (pk int not null primary key comment 'tag:%d');", pkTag)

var SuperSchemaTests = []SuperSchemaTest{
	{
		Name:      "can create super schema",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.CommitAll{Message: "created table testable"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
	},
	{
		Name:      "get super schema without commit",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
	},
	{
		Name:      "add column",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "drop column",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: "alter table testable drop column c0"},
			tc.CommitAll{Message: "dropped column c0"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "modify column",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: "alter table testable drop column c0"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "drop column from working set",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.Query{Query: "alter table testable drop column c0"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
	},
	{
		Name:      "staged column persisted on commit, not working column",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.StageAll{},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			tc.CommitStaged{Message: "adding staged column c0"},
			tc.ResetHard{},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "super schema on branch master",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "super schema on branch other",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			tc.CommitAll{Message: "added column c1 on branch master"},
			tc.Checkout{BranchName: "other"},
		},
		ExpectedBranch: "other",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
		)),
	},
	// https://github.com/liquidata-inc/dolt/issues/773
	/*{
		Name:      "super schema merge",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			tc.CommitAll{Message: "added column c1 on branch master"},
			tc.Merge{BranchName: "other"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "super schema merge with drops",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c12 int comment 'tag:%d';", c12Tag)},
			tc.CommitAll{Message: "added columns c11 and c12 on branch other"},
			tc.Query{Query: "alter table testable drop column c12;"},
			tc.CommitAll{Message: "dropped column c12 on branch other"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			tc.CommitAll{Message: "added column c1 on branch master"},
			tc.Merge{BranchName: "other"},
			tc.CommitAll{Message: "Merged other into master"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c11", c11Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c12", c12Tag, typeinfo.Int32Type, false),
		)),
	},*/
	{
		Name:      "super schema with table add/drops",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			tc.Query{Query: "create table foo (pk int not null primary key);"},
			tc.CommitAll{Message: "created tables testable and foo"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			tc.Query{Query: "create table qux (pk int not null primary key);"},
			tc.Query{Query: "drop table foo;"},
			tc.CommitAll{Message: "added column c1 on branch master, created table qux, dropped table foo"},
		},
		ExpectedBranch: "master",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
			newColTypeInfo("c1", c1Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		// This test corresponds to @test "diff sql reconciles DROP TABLE" in sql_diff.bats
		Name:      "sql diff bats test",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Branch{BranchName: "first"},
			tc.Checkout{BranchName: "first"},
			tc.Query{Query: testableDef},
			tc.Query{Query: "insert into testable values (1);"},
			tc.CommitAll{Message: "setup table"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: "drop table testable;"},
			tc.CommitAll{Message: "removed table"},
			tc.Checkout{BranchName: "first"},
		},
		ExpectedBranch: "first",
		ExpectedSchema: schema.SchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
		)),
	},
}

func TestSuperSchema(t *testing.T) {
	for _, test := range SuperSchemaTests {
		t.Run(test.Name, func(t *testing.T) {
			testSuperSchema(t, test)
		})
	}
}

func testSuperSchema(t *testing.T, test SuperSchemaTest) {
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

		ss, found, err := r.GetSuperSchema(context.Background(), test.TableName)
		require.True(t, found)
		require.NoError(t, err)
		assert.Equal(t, test.ExpectedSuperSchema, ss)

		sch, err := tbl.GetSchema(context.Background())
		require.NoError(t, err)
		assert.Equal(t, test.ExpectedSchema, sch)
	}
}

func superSchemaFromCols(cols *schema.ColCollection) *schema.SuperSchema {
	sch := schema.SchemaFromCols(cols)
	ss, _ := schema.NewSuperSchema(sch)
	return ss
}
