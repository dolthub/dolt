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
	"time"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
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
	Commands []Command
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
		Commands: []Command{
			Query{testableDef},
			CommitAll{"created table testable"},
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
		Commands: []Command{
			Query{testableDef},
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
		Commands: []Command{
			Query{testableDef},
			CommitAll{"created table testable"},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Query{"alter table testable drop column c0"},
			CommitAll{"dropped column c0"},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Query{"alter table testable drop column c0"},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			Query{"alter table testable drop column c0"},
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
		Commands: []Command{
			Query{testableDef},
			CommitAll{"created table testable"},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			AddAll{},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			CommitStaged{"adding staged column c0"},
			ResetHard{},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Branch{"other"},
			Checkout{"other"},
			Query{fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			CommitAll{"added column c11 on branch other"},
			Checkout{"master"},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Branch{"other"},
			Checkout{"other"},
			Query{fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			CommitAll{"added column c11 on branch other"},
			Checkout{"master"},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			CommitAll{"added column c1 on branch master"},
			Checkout{"other"},
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
	{
		Name:      "super schema merge",
		TableName: "testable",
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Branch{"other"},
			Checkout{"other"},
			Query{fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			CommitAll{"added column c11 on branch other"},
			Checkout{"master"},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			CommitAll{"added column c1 on branch master"},
			Merge{"other"},
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
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			CommitAll{"created table testable"},
			Branch{"other"},
			Checkout{"other"},
			Query{fmt.Sprintf("alter table testable add column c11 int comment 'tag:%d';", c11Tag)},
			Query{fmt.Sprintf("alter table testable add column c12 int comment 'tag:%d';", c12Tag)},
			CommitAll{"added columns c11 and c12 on branch other"},
			Query{"alter table testable drop column c12;"},
			CommitAll{"dropped column c12 on branch other"},
			Checkout{"master"},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			CommitAll{"added column c1 on branch master"},
			Merge{"other"},
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
	},
	{
		Name:      "super schema with table add/drops",
		TableName: "testable",
		Commands: []Command{
			Query{testableDef},
			Query{fmt.Sprintf("alter table testable add column c0 int comment 'tag:%d';", c0Tag)},
			Query{"create table foo (pk int not null primary key);"},
			CommitAll{"created tables testable and foo"},
			Query{fmt.Sprintf("alter table testable add column c1 int comment 'tag:%d';", c1Tag)},
			Query{"create table qux (pk int not null primary key);"},
			Query{"drop table foo;"},
			CommitAll{"added column c1 on branch master, created table qux, dropped table foo"},
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
		Commands: []Command{
			Branch{"first"},
			Checkout{"first"},
			Query{testableDef},
			Query{"insert into testable values (1);"},
			CommitAll{"setup table"},
			Branch{"other"},
			Checkout{"other"},
			Query{"drop table testable;"},
			CommitAll{"removed table"},
			Checkout{"first"},
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
	for _, cmd := range test.Commands {
		cmd.Exec(t, dEnv)
	}

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

func superSchemaFromCols(cols *schema.ColCollection) *schema.SuperSchema {
	sch := schema.SchemaFromCols(cols)
	ss, _ := schema.NewSuperSchema(sch)
	return ss
}

func columnCollection(cols ...schema.Column) *schema.ColCollection {
	pcc, _ := schema.NewColCollection(cols...)
	return pcc
}

func newColTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	c, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, constraints...)
	if err != nil {
		panic("could not create column")
	}
	return c
}

type Command interface {
	Exec(t *testing.T, dEnv *env.DoltEnv)
}

type AddAll struct{}

func (c AddAll) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)
}

type CommitStaged struct {
	Message string
}

func (c CommitStaged) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.CommitStaged(context.Background(), dEnv, c.Message, time.Now(), false)
	require.NoError(t, err)
	cm, _ := commands.ResolveCommitWithVErr(dEnv, "HEAD", dEnv.RepoState.Head.Ref.String())
	ch, _ := cm.HashOf()
	fmt.Println(fmt.Sprintf("commit: %s", ch.String()))
}

type CommitAll struct {
	Message string
}

func (c CommitAll) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)
	err = actions.CommitStaged(context.Background(), dEnv, c.Message, time.Now(), false)
	require.NoError(t, err)
	cm, _ := commands.ResolveCommitWithVErr(dEnv, "HEAD", dEnv.RepoState.Head.Ref.String())
	ch, _ := cm.HashOf()
	fmt.Println(fmt.Sprintf("commit: %s", ch.String()))
}

type Query struct {
	Query string
}

func (q Query) Exec(t *testing.T, dEnv *env.DoltEnv) {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	sqlDb := dsqle.NewDatabase("dolt", root, nil, nil)
	engine := sqle.NewDefault()
	engine.AddDatabase(sqlDb)
	err = engine.Init()
	require.NoError(t, err)
	sqlCtx := sql.NewContext(context.Background())
	_, _, err = engine.Query(sqlCtx, q.Query)
	require.NoError(t, err)
	err = dEnv.UpdateWorkingRoot(context.Background(), sqlDb.Root())
	require.NoError(t, err)
}

type Branch struct {
	branchName string
}

func (b Branch) Exec(t *testing.T, dEnv *env.DoltEnv) {
	cwb := dEnv.RepoState.Head.Ref.String()
	err := actions.CreateBranch(context.Background(), dEnv, b.branchName, cwb, false)
	require.NoError(t, err)
}

type Checkout struct {
	BranchName string
}

func (c Checkout) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.CheckoutBranch(context.Background(), dEnv, c.BranchName)
	require.NoError(t, err)
}

type Merge struct {
	BranchName string
}

func (m Merge) Exec(t *testing.T, dEnv *env.DoltEnv) {
	mm := commands.MergeCmd{}
	status := mm.Exec(context.Background(), "dolt merge", []string{m.BranchName}, dEnv)
	assert.Equal(t, 0, status)
}

type ResetHard struct{}

// NOTE: does not handle untracked tables
func (r ResetHard) Exec(t *testing.T, dEnv *env.DoltEnv) {
	headRoot, err := dEnv.HeadRoot(context.Background())
	require.NoError(t, err)

	err = dEnv.UpdateWorkingRoot(context.Background(), headRoot)
	require.NoError(t, err)

	_, err = dEnv.UpdateStagedRoot(context.Background(), headRoot)
	require.NoError(t, err)

	err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
	require.NoError(t, err)
}
