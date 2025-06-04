// Copyright 2022 Dolthub, Inc.
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

package dprocedures

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	includeUntrackedFlag = "include-untracked"
)

// doltStash is the stored procedure version for the CLI command `dolt stash`
// and its options push, pop, drop, and clear
func doltStash(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltStash(ctx, args)
	if err != nil {
		return nil, err
	}

	return rowToIter(res), nil
}

func doDoltStash(ctx *sql.Context, args []string) (string, error) {
	dbName := ctx.GetCurrentDatabase()

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return "", fmt.Errorf("Could not load database %s", dbName)
	}
	if !dbData.Ddb.Format().UsesFlatbuffers() {
		return "", fmt.Errorf("stash is not supported for old storage format")
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return "", fmt.Errorf("Could not load roots for database %s", dbName)
	}

	apr, err := cli.CreateStashArgParser().Parse(args)
	if err != nil {
		return "", err
	}

	if apr.NArg() == 0 {
		return "", fmt.Errorf("error: invalid arguments. Must provide valid stash subcommand")
	}

	var status string
	switch apr.Arg(0) {
	case "push":
		status, err = doStashPush(ctx, dSess, dbData, roots, apr)
	case "pop":
		status, err = doStashPop(ctx, dbData, apr)
	case "drop":
		status, err = doStashDrop(ctx, dbData, apr)
	case "clear":
		err = doStashClear(ctx, dbData)
	default:
		return "", fmt.Errorf("unknown stash subcommand %s", apr.Arg(0))
	}

	if err != nil {
		return "", err
	}

	return status, nil
}

func doStashPush(ctx *sql.Context, dSess *dsess.DoltSession, dbData env.DbData[*sql.Context], roots doltdb.Roots, apr *argparser.ArgParseResults) (string, error) {

	hasChanges, err := hasLocalChanges(ctx, dSess, dbData, roots, apr)
	if err != nil {
		return "", err
	}
	if !hasChanges {
		return "", fmt.Errorf("no local changes to save")
	}

	roots, err = actions.StageModifiedAndDeletedTables(ctx, roots)
	if err != nil {
		return "", err
	}

	// all tables with changes that are going to be stashed are staged at this point

	allTblsToBeStashed, addedTblsToStage, err := stashedTableSets(ctx, roots)
	if err != nil {
		return "", err
	}

	if apr.Contains(includeUntrackedFlag) || apr.Contains(cli.AllFlag) {
		allTblsToBeStashed, err = doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
		if err != nil {
			return "", err
		}

		roots, err = actions.StageTables(ctx, roots, allTblsToBeStashed, !apr.Contains("all"))
		if err != nil {
			return "", err
		}
	}

	curHeadRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return "", err
	}

	curBranchName := curHeadRef.String()
	commitSpec, err := doltdb.NewCommitSpec(curBranchName)
	if err != nil {
		return "", err
	}
	optCmt, err := dbData.Ddb.Resolve(ctx, commitSpec, curHeadRef)
	if err != nil {
		return "", err
	}
	commit, ok := optCmt.ToCommit()
	if !ok {
		return "", doltdb.ErrGhostCommitEncountered
	}

	commitMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}

	err = dbData.Ddb.AddStash(ctx, commit, roots.Staged, datas.NewStashMeta(curBranchName, commitMeta.Description, doltdb.FlattenTableNames(addedTblsToStage)), "stashes")
	if err != nil {
		return "", err
	}

	roots.Staged = roots.Head
	roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, allTblsToBeStashed)
	if err != nil {
		return "", err
	}

	err = updateWorkingSetFromRoots(ctx, dbData, roots)
	if err != nil {
		return "", err
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return "", err
	}

	status := fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", curBranchName, commitHash.String(), commitMeta.Description)
	return status, nil
}

func doStashPop(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults) (string, error) {
	var idx = 0
	var err error
	if apr.NArg() == 2 {
		idx, err = parseStashIndex(apr.Args[1])
	}

	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return "", err
	}
	workingSetRef, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return "", err
	}
	workingSet, err := dbData.Ddb.ResolveWorkingSet(ctx, workingSetRef)
	if err != nil {
		return "", err
	}
	curWorkingRoot := workingSet.WorkingRoot()

	stashRoot, headCommit, meta, err := dbData.Ddb.GetStashRootAndHeadCommitAtIdx(ctx, idx, "stashes")
	if err != nil {
		return "", err
	}

	hch, err := headCommit.HashOf()
	if err != nil {
		return "", err
	}
	headCommitSpec, err := doltdb.NewCommitSpec(hch.String())
	if err != nil {
		return "", err
	}

	optCmt, err := dbData.Ddb.Resolve(ctx, headCommitSpec, headRef)
	if err != nil {
		return "", err
	}
	parentCommit, ok := optCmt.ToCommit()
	if !ok {
		// Should not be possible to get into this situation. The parent of the stashed commit
		// Must have been present at the time it was created
		return "", doltdb.ErrGhostCommitEncountered
	}

	parentRoot, err := parentCommit.GetRootValue(ctx)
	if err != nil {
		return "", err
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return "", err
	}

	opts := editor.Options{Deaf: bulkDbEaFactory(dbData), Tempdir: tmpDir}
	result, err := merge.MergeRoots(ctx, curWorkingRoot, stashRoot, parentRoot, stashRoot, parentCommit, opts, merge.MergeOpts{IsCherryPick: false})
	if err != nil {
		return "", err
	}

	var tablesWithConflict []doltdb.TableName
	for tbl, stats := range result.Stats {
		if stats.HasConflicts() {
			tablesWithConflict = append(tablesWithConflict, tbl)
		}
	}

	if len(tablesWithConflict) > 0 {
		tblNames := strings.Join(doltdb.FlattenTableNames(tablesWithConflict), "', '")
		status := fmt.Sprintf("error: Your local changes to the following tables would be overwritten by applying stash %d:\n"+
			"\t{'%s'}\n"+
			"Please commit your changes or stash them before you merge.\nAborting\n", idx, tblNames)
		return status, nil
	}

	err = updateWorkingRoot(ctx, dbData, result.Root)
	if err != nil {
		return "", err
	}

	headRoot, err := headCommit.GetRootValue(ctx)
	if err != nil {
		return "", err
	}
	ws, err := env.WorkingSet(ctx, dbData.Ddb, dbData.Rsr)
	if err != nil {
		return "", err
	}
	roots := doltdb.Roots{
		Head:    headRoot,
		Working: ws.WorkingRoot(),
		Staged:  ws.StagedRoot(),
	}

	// added tables need to be staged
	// since these tables are coming from a stash, don't filter for ignored table names.
	roots, err = actions.StageTables(ctx, roots, doltdb.ToTableNames(meta.TablesToStage, doltdb.DefaultSchemaName), false)
	if err != nil {
		return "", err
	}

	err = updateWorkingSetFromRoots(ctx, dbData, roots)
	if err != nil {
		return "", err
	}

	stashHash, err := dbData.Ddb.GetStashHashAtIdx(ctx, idx, "stashes")
	if err != nil {
		return "", err
	}

	err = dbData.Ddb.RemoveStashAtIdx(ctx, idx, "stashes")
	if err != nil {
		return "", err
	}

	status := fmt.Sprintf("Dropped refs/stash@{%v} (%s)", idx, stashHash.String())
	return status, err
}

func doStashDrop(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults) (string, error) {
	var idx = 0
	var err error

	if apr.NArg() == 2 {
		idx, err = parseStashIndex(apr.Args[1])
		if err != nil {
			return "", err
		}
	}

	stashHash, err := dbData.Ddb.GetStashHashAtIdx(ctx, idx, "stashes")
	if err != nil {
		return "", err
	}

	err = dbData.Ddb.RemoveStashAtIdx(ctx, idx, "stashes")
	if err != nil {
		return "", err
	}

	status := fmt.Sprintf("Dropped refs/stash@{%v} (%s)", idx, stashHash.String())

	return status, nil
}

func doStashClear(ctx *sql.Context, dbData env.DbData[*sql.Context]) error {
	err := dbData.Ddb.RemoveAllStashes(ctx, "stashes")
	if err != nil {
		return err
	}
	return nil
}

func stashedTableSets(ctx context.Context, roots doltdb.Roots) ([]doltdb.TableName, []doltdb.TableName, error) {
	var addedTblsInStaged []doltdb.TableName
	var allTbls []doltdb.TableName
	staged, _, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, nil, err
	}

	for _, tableDelta := range staged {
		tblName := tableDelta.ToName
		if tableDelta.IsAdd() {
			addedTblsInStaged = append(addedTblsInStaged, tableDelta.ToName)
		}
		if tableDelta.IsDrop() {
			tblName = tableDelta.FromName
		}
		allTbls = append(allTbls, tblName)
	}

	return allTbls, addedTblsInStaged, nil
}

func hasLocalChanges(ctx *sql.Context, dSess *dsess.DoltSession, dbData env.DbData[*sql.Context], roots doltdb.Roots, apr *argparser.ArgParseResults) (bool, error) {
	dbName := ctx.GetCurrentDatabase()

	headCommit, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return false, err
	}

	headRoot, err := headCommit.GetRootValue(ctx)
	if err != nil {
		return false, err
	}

	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return false, err
	}
	workingRoot := workingSet.WorkingRoot()
	stagedRoot := workingSet.StagedRoot()

	headHash, err := headRoot.HashOf()
	if err != nil {
		return false, err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return false, err
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return false, err
	}

	// Are there staged changes? If so, stash them.
	if !headHash.Equal(stagedHash) {
		return true, nil
	}

	// No staged changes, but are there any unstaged changes? If not, no work is needed.
	if headHash.Equal(workingHash) {
		return false, nil
	}

	// There are unstaged changes, is --all set? If so, nothing else matters. Stash them.
	if apr.Contains(cli.AllFlag) {
		return true, nil
	}

	// --all was not set, so we can ignore tables. Is every table ignored?
	allIgnored, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
	if err != nil {
		return false, err
	}

	if allIgnored {
		return false, nil
	}

	// There are unignored, unstaged tables. Is --include-untracked set? If so, nothing else matters. Stash them.
	if apr.Contains(includeUntrackedFlag) {
		return true, nil
	}

	// --include-untracked was not set, so we can skip untracked tables. Is every table untracked?
	allUntracked, err := workingSetContainsOnlyUntrackedTables(ctx, roots)
	if err != nil {
		return false, err
	}

	if allUntracked {
		return false, nil
	}

	// There are changes to tracked tables. Stash them.
	return true, nil
}

func workingSetContainsOnlyUntrackedTables(ctx context.Context, roots doltdb.Roots) (bool, error) {
	_, unstaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return false, err
	}

	// All ignored files are also untracked files
	for _, tableDelta := range unstaged {
		if !tableDelta.IsAdd() {
			return false, nil
		}
	}

	return true, nil
}

func updateWorkingSetFromRoots(ctx *sql.Context, dbData env.DbData[*sql.Context], roots doltdb.Roots) error {
	ws, err := env.WorkingSet(ctx, dbData.Ddb, dbData.Rsr)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating roots
		headRef, err := dbData.Rsr.CWBHeadRef(ctx)
		if err != nil {
			return err
		}
		wsRef, err := ref.WorkingSetRefForHead(headRef)
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged)

	currentWs, err := env.WorkingSet(ctx, dbData.Ddb, dbData.Rsr)
	if err != doltdb.ErrWorkingSetNotFound && err != nil {
		return err
	}

	var h hash.Hash
	if currentWs != nil {
		h, err = currentWs.HashOf()
		if err != nil {
			return err
		}
	}

	wsm := &datas.WorkingSetMeta{
		Timestamp:   uint64(time.Now().Unix()),
		Description: "updated from dolt environment",
	}

	err = dbData.Ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, h, wsm, nil)
	if err != nil {
		return err
	}

	return nil
}

func parseStashIndex(stashName string) (int, error) {
	var idx = 0
	stashName = strings.TrimSuffix(strings.TrimPrefix(stashName, "stash@{"), "}")
	idx, err := strconv.Atoi(stashName)
	if err != nil {
		return 0, fmt.Errorf("error: %s is not a valid reference", stashName)
	}

	return idx, nil
}

func bulkDbEaFactory(dbData env.DbData[*sql.Context]) editor.DbEaFactory {
	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return nil
	}
	return editor.NewBulkImportTEAFactory(dbData.Ddb.ValueReadWriter(), tmpDir)
}

func updateWorkingRoot(ctx *sql.Context, dbData env.DbData[*sql.Context], newRoot doltdb.RootValue) error {
	//err = dEnv.UpdateWorkingRoot(ctx, result.Root)
	var h hash.Hash
	var wsRef ref.WorkingSetRef
	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return err
	}

	ws, err := env.WorkingSet(ctx, dbData.Ddb, dbData.Rsr)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(headRef)
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(newRoot).WithStagedRoot(newRoot)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	wsm := &datas.WorkingSetMeta{
		Timestamp:   uint64(time.Now().Unix()),
		Description: "updated from dolt environment",
	}

	err = dbData.Ddb.UpdateWorkingSet(ctx, wsRef, ws.WithWorkingRoot(newRoot), h, wsm, nil)
	if err != nil {
		return err
	}

	return nil
}
