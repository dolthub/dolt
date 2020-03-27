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

package testcommands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/merge"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
)

type Command interface {
	CommandName() string
	Exec(t *testing.T, dEnv *env.DoltEnv)
}

type Commit struct {
	Message string
}

// CommandName returns "commit".
func (c Commit) CommandName() string { return "commit" }

// Exec executes a Commit command on a test dolt environment.
func (c Commit) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.StageAllTables(context.Background(), dEnv, false)
	require.NoError(t, err)

	//stagedTbls, notStagedTbls, err := diff.GetTableDiffs(context.Background(), dEnv)
	//require.NoError(t, err)

	var mergeCmSpec []*doltdb.CommitSpec
	if dEnv.IsMergeActive() {
		spec, err := doltdb.NewCommitSpec(dEnv.RepoState.Merge.Commit, dEnv.RepoState.Merge.Head.Ref.String())

		if err != nil {
			panic("Corrupted repostate. Active merge state is not valid.")
		}

		mergeCmSpec = []*doltdb.CommitSpec{spec}
	}

	root, err := dEnv.StagedRoot(context.Background())
	assert.NoError(t, err)

	h, err := dEnv.UpdateStagedRoot(context.Background(), root)
	assert.NoError(t, err)

	_, err = dEnv.DoltDB.CommitWithParentSpecs(context.Background(), h, dEnv.RepoState.CWBHeadRef(), mergeCmSpec, &doltdb.CommitMeta{})
	assert.NoError(t, err)

	//cm := resolveCommit(t, "HEAD", dEnv)
	//ch, _ := cm.HashOf()
	//fmt.Println(fmt.Sprintf("commit: %s", ch.String()))
}

type Query struct {
	Query string
}

// CommandName returns "query".
func (q Query) CommandName() string { return "query" }

// Exec executes a Query command on a test dolt environment.
func (q Query) Exec(t *testing.T, dEnv *env.DoltEnv) {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	sqlDb := dsqle.NewDatabase("dolt", root, dEnv.DoltDB, nil)
	engine, sqlCtx, err := dsqle.NewTestEngine(context.Background(), sqlDb, root)
	require.NoError(t, err)

	_, _, err = engine.Query(sqlCtx, q.Query)
	require.NoError(t, err)
	newRoot, err := sqlDb.GetRoot(sqlCtx)
	require.NoError(t, err)
	err = dEnv.UpdateWorkingRoot(sqlCtx, newRoot)
	require.NoError(t, err)
}

type Branch struct {
	BranchName string
}

// CommandName returns "branch".
func (b Branch) CommandName() string { return "branch" }

// Exec executes a Branch command on a test dolt environment.
func (b Branch) Exec(t *testing.T, dEnv *env.DoltEnv) {
	cwb := dEnv.RepoState.Head.Ref.String()
	err := actions.CreateBranch(context.Background(), dEnv, b.BranchName, cwb, false)
	require.NoError(t, err)
}

type Checkout struct {
	BranchName string
}

// CommandName returns "checkout".
func (c Checkout) CommandName() string { return "checkout" }

// Exec executes a Checkout command on a test dolt environment.
func (c Checkout) Exec(t *testing.T, dEnv *env.DoltEnv) {
	err := actions.CheckoutBranch(context.Background(), dEnv, c.BranchName)
	require.NoError(t, err)
}

type Merge struct {
	BranchName string
}

// CommandName returns "merge".
func (m Merge) CommandName() string { return "merge" }

// Exec executes a Merge command on a test dolt environment.
func (m Merge) Exec(t *testing.T, dEnv *env.DoltEnv) {
	// Adapted from commands/merge.go:Exec()
	dref, err := dEnv.FindRef(context.Background(), m.BranchName)
	assert.NoError(t, err)

	cm1 := resolveCommit(t, "HEAD", dEnv)
	cm2 := resolveCommit(t, dref.String(), dEnv)

	h1, err := cm1.HashOf()
	assert.NoError(t, err)

	h2, err := cm2.HashOf()
	assert.NoError(t, err)
	assert.NotEqual(t, h1, h2)

	tblNames, _, err := dEnv.MergeWouldStompChanges(context.Background(), cm2)
	assert.NoError(t, err)
	assert.True(t, len(tblNames) == 0)

	if ok, err := cm1.CanFastForwardTo(context.Background(), cm2); ok {
		assert.NoError(t, err)

		rv, err := cm2.GetRootValue()
		assert.NoError(t, err)

		h, err := dEnv.DoltDB.WriteRootValue(context.Background(), rv)
		assert.NoError(t, err)

		err = dEnv.DoltDB.FastForward(context.Background(), dEnv.RepoState.CWBHeadRef(), cm2)
		assert.NoError(t, err)

		dEnv.RepoState.Working = h.String()
		dEnv.RepoState.Staged = h.String()
		err = dEnv.RepoState.Save(dEnv.FS)
		assert.NoError(t, err)

		err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
		assert.NoError(t, err)

	} else {
		mergedRoot, tblToStats, err := merge.MergeCommits(context.Background(), dEnv.DoltDB, cm1, cm2)
		for _, stats := range tblToStats {
			assert.True(t, stats.Conflicts == 0)
		}

		h2, err := cm2.HashOf()
		assert.NoError(t, err)

		err = dEnv.RepoState.StartMerge(dref, h2.String(), dEnv.FS)
		assert.NoError(t, err)

		err = dEnv.UpdateWorkingRoot(context.Background(), mergedRoot)
		assert.NoError(t, err)

		err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
		assert.NoError(t, err)

		_, err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot)
		assert.NoError(t, err)
	}
}

func resolveCommit(t *testing.T, cSpecStr string, dEnv *env.DoltEnv) *doltdb.Commit {
	cs, err := doltdb.NewCommitSpec(cSpecStr, dEnv.RepoState.Head.Ref.String())
	require.NoError(t, err)
	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs)
	require.NoError(t, err)
	return cm
}
