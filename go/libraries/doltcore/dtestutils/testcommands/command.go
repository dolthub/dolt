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

// Package testcommands TODO: kill off this package, replace with the non-test commands directly
package testcommands

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type Command interface {
	CommandString() string
	Exec(t *testing.T, dEnv *env.DoltEnv) error
}

type StageAll struct{}

// CommandString describes the StageAll command for debugging purposes.
func (a StageAll) CommandString() string { return "stage_all" }

// Exec executes a StageAll command on a test dolt environment.
func (a StageAll) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	roots, err := dEnv.Roots(context.Background())
	require.NoError(t, err)

	roots, err = actions.StageAllTables(context.Background(), roots, dEnv.Docs)
	require.NoError(t, err)

	return dEnv.UpdateRoots(context.Background(), roots)
}

type CommitStaged struct {
	Message string
}

// CommandString describes the CommitStaged command for debugging purposes.
func (c CommitStaged) CommandString() string { return fmt.Sprintf("commit_staged: %s", c.Message) }

// Exec executes a CommitStaged command on a test dolt environment.
func (c CommitStaged) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	roots, err := dEnv.Roots(context.Background())
	require.NoError(t, err)

	name, email, err := env.GetNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	dbData := dEnv.DbData()

	ws, err := dEnv.WorkingSet(context.Background())
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = actions.CommitStaged(context.Background(), roots, ws.MergeActive(), mergeParentCommits, dbData, actions.CommitStagedProps{
		Message:    c.Message,
		Date:       time.Now(),
		AllowEmpty: false,
		Force:      false,
		Name:       name,
		Email:      email,
	})

	if err != nil {
		return err
	}

	return dEnv.ClearMerge(context.Background())
}

type CommitAll struct {
	Message string
}

// CommandString describes the CommitAll command for debugging purposes.
func (c CommitAll) CommandString() string { return fmt.Sprintf("commit: %s", c.Message) }

// Exec executes a CommitAll command on a test dolt environment.
func (c CommitAll) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	roots, err := dEnv.Roots(context.Background())
	require.NoError(t, err)

	roots, err = actions.StageAllTables(context.Background(), roots, dEnv.Docs)
	require.NoError(t, err)

	name, email, err := env.GetNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	ws, err := dEnv.WorkingSet(context.Background())
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = actions.CommitStaged(context.Background(), roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    c.Message,
		Date:       time.Now(),
		AllowEmpty: false,
		Force:      false,
		Name:       name,
		Email:      email,
	})

	if err != nil {
		return err
	}

	return dEnv.ClearMerge(context.Background())
}

type ResetHard struct{}

// CommandString describes the ResetHard command for debugging purposes.
func (r ResetHard) CommandString() string { return "reset_hard" }

// NOTE: does not handle untracked tables
func (r ResetHard) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	headRoot, err := dEnv.HeadRoot(context.Background())
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingRoot(context.Background(), headRoot)
	if err != nil {
		return err
	}

	err = dEnv.UpdateStagedRoot(context.Background(), headRoot)
	if err != nil {
		return err
	}

	err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
	return err
}

type Query struct {
	Query string
}

// CommandString describes the Query command for debugging purposes.
func (q Query) CommandString() string { return fmt.Sprintf("query %s", q.Query) }

// Exec executes a Query command on a test dolt environment.
func (q Query) Exec(t *testing.T, dEnv *env.DoltEnv) error {
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	sqlDb := dsqle.NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := dsqle.NewTestEngine(t, dEnv, context.Background(), sqlDb, root)
	require.NoError(t, err)

	_, iter, err := engine.Query(sqlCtx, q.Query)
	if err != nil {
		return err
	}

	for {
		_, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	err = iter.Close(sqlCtx)
	if err != nil {
		return err
	}

	newRoot, err := sqlDb.GetRoot(sqlCtx)
	require.NoError(t, err)

	err = dEnv.UpdateWorkingRoot(context.Background(), newRoot)
	return err
}

type Branch struct {
	BranchName string
}

// CommandString describes the Branch command for debugging purposes.
func (b Branch) CommandString() string { return fmt.Sprintf("branch: %s", b.BranchName) }

// Exec executes a Branch command on a test dolt environment.
func (b Branch) Exec(_ *testing.T, dEnv *env.DoltEnv) error {
	cwb := dEnv.RepoStateReader().CWBHeadRef().String()
	return actions.CreateBranchWithStartPt(context.Background(), dEnv.DbData(), b.BranchName, cwb, false)
}

type Checkout struct {
	BranchName string
}

// CommandString describes the Checkout command for debugging purposes.
func (c Checkout) CommandString() string { return fmt.Sprintf("checkout: %s", c.BranchName) }

// Exec executes a Checkout command on a test dolt environment.
func (c Checkout) Exec(_ *testing.T, dEnv *env.DoltEnv) error {
	return actions.CheckoutBranch(context.Background(), dEnv, c.BranchName, false)
}

type Merge struct {
	BranchName string
}

// CommandString describes the Merge command for debugging purposes.
func (m Merge) CommandString() string { return fmt.Sprintf("merge: %s", m.BranchName) }

// Exec executes a Merge command on a test dolt environment.
func (m Merge) Exec(t *testing.T, dEnv *env.DoltEnv) error {
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

	roots, err := dEnv.Roots(context.Background())
	require.NoError(t, err)

	tblNames, _, err := merge.MergeWouldStompChanges(context.Background(), roots, cm2)
	if err != nil {
		return err
	}
	if len(tblNames) != 0 {
		return errhand.BuildDError("error: failed to determine mergability.").AddCause(err).Build()
	}

	if ok, err := cm1.CanFastForwardTo(context.Background(), cm2); ok {
		if err != nil {
			return err
		}

		err = dEnv.DoltDB.FastForward(context.Background(), dEnv.RepoStateReader().CWBHeadRef(), cm2)
		if err != nil {
			return err
		}

		workingSet, err := dEnv.WorkingSet(context.Background())
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		rv, err := cm2.GetRootValue()
		assert.NoError(t, err)

		err = dEnv.UpdateWorkingSet(context.Background(), workingSet.WithWorkingRoot(rv))
		require.NoError(t, err)

		err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
		assert.NoError(t, err)

	} else {
		opts := editor.Options{Deaf: dEnv.DbEaFactory()}
		mergedRoot, tblToStats, err := merge.MergeCommits(context.Background(), cm1, cm2, opts)
		require.NoError(t, err)
		for _, stats := range tblToStats {
			require.True(t, stats.Conflicts == 0)
		}

		err = dEnv.StartMerge(context.Background(), cm2)
		if err != nil {
			return err
		}

		err = dEnv.UpdateWorkingRoot(context.Background(), mergedRoot)
		if err != nil {
			return err
		}

		err = actions.SaveTrackedDocsFromWorking(context.Background(), dEnv)
		if err != nil {
			return err
		}

		err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveCommit(t *testing.T, cSpecStr string, dEnv *env.DoltEnv) *doltdb.Commit {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	require.NoError(t, err)
	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)
	return cm
}

type ConflictsCat struct {
	TableName string
}

// CommandString describes the ConflictsCat command for debugging purposes.
func (c ConflictsCat) CommandString() string { return fmt.Sprintf("conflicts_cat: %s", c.TableName) }

// Exec executes a ConflictsCat command on a test dolt environment.
func (c ConflictsCat) Exec(t *testing.T, wg *sync.WaitGroup, dEnv *env.DoltEnv) error {
	out := cnfcmds.CatCmd{}.Exec(context.Background(), "dolt conflicts cat", []string{c.TableName}, dEnv)
	require.Equal(t, 0, out)
	return nil
}
