// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	tc "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
)

const (
	pkTag  = 16191
	c0Tag  = 8734
	c1Tag  = 15903
	c11Tag = 15001
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

var testableDef = fmt.Sprintf("create table testable (pk int not null primary key);")

var SuperSchemaTests = []SuperSchemaTest{
	{
		Name:      "can create super schema",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.CommitAll{Message: "created table testable"},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int")},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: "alter table testable drop column c0"},
			tc.CommitAll{Message: "dropped column c0"},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.CommitAll{Message: "created table testable"},
			tc.Query{Query: "alter table testable drop column c0"},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.Query{Query: "alter table testable drop column c0"},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.StageAll{},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
			tc.CommitStaged{Message: "adding staged column c0"},
			tc.ResetHard{},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
		ExpectedSuperSchema: superSchemaFromCols(columnCollection(
			newColTypeInfo("pk", pkTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
			newColTypeInfo("c0", c0Tag, typeinfo.Int32Type, false),
		)),
	},
	{
		Name:      "super schema on branch main",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int;")},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: env.DefaultInitBranch},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int;")},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: env.DefaultInitBranch},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
			tc.CommitAll{Message: "added column c1 on branch main"},
			tc.Checkout{BranchName: "other"},
		},
		ExpectedBranch: "other",
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
	// https://github.com/dolthub/dolt/issues/773
	/*{
		Name:      "super schema merge",
		TableName: "testable",
		Commands: []tc.Command{
			tc.Query{Query: testableDef},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int;")},
			tc.CommitAll{Message: "added column c11 on branch other"},
			tc.Checkout{BranchName: "main"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
			tc.CommitAll{Message: "added column c1 on branch main"},
			tc.Merge{BranchName: "other"},
		},
		ExpectedBranch: "main",
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.CommitAll{Message: "created table testable"},
			tc.Branch{BranchName: "other"},
			tc.Checkout{BranchName: "other"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c11 int;")},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c12 int;")},
			tc.CommitAll{Message: "added columns c11 and c12 on branch other"},
			tc.Query{Query: "alter table testable drop column c12;"},
			tc.CommitAll{Message: "dropped column c12 on branch other"},
			tc.Checkout{BranchName: "main"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
			tc.CommitAll{Message: "added column c1 on branch main"},
			tc.Merge{BranchName: "other"},
			tc.CommitAll{Message: "Merged other into main"},
		},
		ExpectedBranch: "main",
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
			tc.Query{Query: fmt.Sprintf("alter table testable add column c0 int;")},
			tc.Query{Query: "create table foo (pk int not null primary key);"},
			tc.CommitAll{Message: "created tables testable and foo"},
			tc.Query{Query: fmt.Sprintf("alter table testable add column c1 int;")},
			tc.Query{Query: "create table qux (pk int not null primary key);"},
			tc.Query{Query: "drop table foo;"},
			tc.CommitAll{Message: "added column c1 on branch main, created table qux, dropped table foo"},
		},
		ExpectedBranch: env.DefaultInitBranch,
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
		ExpectedSchema: schema.MustSchemaFromCols(columnCollection(
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
		spec := dEnv.RepoStateReader().CWBHeadRef()
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
	sch := schema.MustSchemaFromCols(cols)
	ss, _ := schema.NewSuperSchema(sch)
	return ss
}

func columnCollection(cols ...schema.Column) *schema.ColCollection {
	return schema.NewColCollection(cols...)
}

func newColTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	c, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, "", false, "", constraints...)
	if err != nil {
		panic("could not create column")
	}
	return c
}
