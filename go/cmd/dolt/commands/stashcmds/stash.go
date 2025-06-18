// Copyright 2023 Dolthub, Inc.
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

package stashcmds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/go-mysql-server/sql"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

var ErrStashNotSupportedForOldFormat = errors.New("stash is not supported for old storage format")

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: "Stash the changes in a dirty working directory away.",
	LongDesc: `Use dolt stash when you want to record the current state of the working directory and the index, but want to go back to a clean working directory. 

The command saves your local modifications away and reverts the working directory to match the HEAD commit. The stash entries that are saved away can be listed with 'dolt stash list'.
`,
	Synopsis: []string{
		"", // this is for `dolt stash` itself.
		"list",
		"pop {{.LessThan}}stash{{.GreaterThan}}",
		"clear",
		"drop {{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.IncludeUntrackedFlag, "u", "Untracked tables are also stashed.")
	ap.SupportsFlag(cli.AllFlag, "a", "All tables are stashed, including untracked and ignored tables.")
	return ap
}

// EventType returns the type of the event to log
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateStashArgParser()
	apr, _, terminate, status := commands.ParseArgsOrPrintHelp(ap, commandStr, args, stashDocs)
	if terminate {
		return status
	}
	if len(apr.Args) > 2 {
		cli.PrintErrln(fmt.Errorf("dolt stash takes 2 arguments, received %d", len(apr.Args)))
		return 1
	}

	subcommand := "push"
	if len(apr.Args) > 0 {
		subcommand = strings.ToLower(apr.Arg(0))
	}

	var err error
	switch subcommand {
	case "push":
		err = stashPush(ctx, cliCtx, dEnv, apr, subcommand)
	case "pop", "drop":
		err = stashRemove(ctx, cliCtx, dEnv, apr, subcommand)
	case "list":
		err = stashList(ctx, cliCtx, dEnv)
	case "clear":
		err = stashClear(ctx, cliCtx, apr, subcommand)
	}

	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	return 0
}

func stashPush(ctx context.Context, cliCtx cli.CliContext, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, subcommand string) error {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return err
	}

	hasChanges, err := hasLocalChanges(ctx, dEnv, roots, apr)
	if err != nil {
		return err
	}
	if !hasChanges {
		cli.Println("No local changes to save")
		return nil
	}
	curBranchName, commit, commitMeta, err := getStashPushData(ctx, dEnv)
	if err != nil {
		return err
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return err
	}

	rowIter, sqlCtx, closeFunc, err := stashQuery(ctx, cliCtx, apr, subcommand)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", curBranchName, commitHash.String(), commitMeta.Description))
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func stashRemove(ctx context.Context, cliCtx cli.CliContext, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, subcommand string) error {
	idx, err := parseStashIndex(apr)
	if err != nil {
		return err
	}
	commitHash, err := dEnv.DoltDB(ctx).GetStashHashAtIdx(ctx, idx, doltdb.DoltCliRef)
	if err != nil {
		return err
	}

	rowIter, sqlCtx, closeFunc, err := stashQuery(ctx, cliCtx, apr, subcommand)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	if subcommand == "pop" {
		ret := commands.StatusCmd{}.Exec(sqlCtx, "status", []string{}, dEnv, cliCtx)
		if ret != 0 {
			cli.Println("The stash entry is kept in case you need it again.")
			return err
		}
	}

	cli.Println(fmt.Sprintf("Dropped refs/stash@{%v} (%s)", idx, commitHash.String()))
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func stashList(ctx context.Context, cliCtx cli.CliContext, dEnv *env.DoltEnv) error {
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	stashes, err := getStashesSQL(ctx, sqlCtx, queryist, dEnv)
	for _, stash := range stashes {
		commitHash, err := stash.HeadCommit.HashOf()
		if err != nil {
			return err
		}
		cli.Println(fmt.Sprintf("%s: WIP on %s: %s %s", stash.Name, stash.BranchReference, commitHash.String(), stash.Description))
	}

	return nil
}

func stashClear(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) error {
	rowIter, sqlCtx, closeFunc, err := stashQuery(ctx, cliCtx, apr, subcommand)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func getStashesSQL(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv) ([]*doltdb.Stash, error) {
	qry := fmt.Sprintf("select stash_id, branch, hash, commit_message from dolt_stashes where name = '%s'", doltdb.DoltCliRef)
	rows, err := commands.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return nil, err
	}

	var stashes []*doltdb.Stash
	for _, s := range rows {
		id, ok := s[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash id")
		}

		branch, ok := s[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash branch")
		}
		fullBranch := ref.NewBranchRef(branch).String()

		stashHash, ok := s[2].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash hash")
		}
		maybeCommit, err := actions.MaybeGetCommit(ctx, dEnv, stashHash)
		if err != nil {
			return nil, err
		}

		msg, ok := s[3].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash message")
		}
		stashes = append(stashes, &doltdb.Stash{
			Name:            id,
			BranchReference: fullBranch,
			HeadCommit:      maybeCommit,
			Description:     msg,
		})
	}

	return stashes, nil
}

// generateStashSql returns the query that will call the `DOLT_STASH` stored proceudre.
func generateStashSql(apr *argparser.ArgParseResults, subcommand string) string {
	var buffer bytes.Buffer
	first := true
	buffer.WriteString("CALL DOLT_STASH(")

	write := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'")
		buffer.WriteString(s)
		buffer.WriteString("'")
		first = false
	}

	write(subcommand)
	write(doltdb.DoltCliRef)

	if len(apr.Args) == 2 {
		// Add stash identifier (i.e. "stash@{0}")
		write(apr.Arg(1))
	}

	if apr.Contains(cli.AllFlag) {
		write("-a")
	}
	if apr.Contains(cli.IncludeUntrackedFlag) {
		write("-u")
	}

	buffer.WriteString(")")
	return buffer.String()
}

func hasLocalChanges(ctx context.Context, dEnv *env.DoltEnv, roots doltdb.Roots, apr *argparser.ArgParseResults) (bool, error) {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return false, err
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return false, err
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return false, err
	}
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

	// There are unignored, unstaged tables. Is --include-untracked set. If so, nothing else matters. Stash them.
	if apr.Contains(cli.IncludeUntrackedFlag) {
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

// getStashPushData is a helper function that returns the current branch, the commit itself, and the commit meta for the most recent commit
func getStashPushData(ctx context.Context, dEnv *env.DoltEnv) (string, *doltdb.Commit, *datas.CommitMeta, error) {
	curHeadRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	curBranchName := curHeadRef.String()
	commitSpec, err := doltdb.NewCommitSpec(curBranchName)
	if err != nil {
		return "", nil, nil, err
	}
	optCmt, err := dEnv.DoltDB(ctx).Resolve(ctx, commitSpec, curHeadRef)
	if err != nil {
		return "", nil, nil, err
	}
	commit, ok := optCmt.ToCommit()
	if !ok {
		return "", nil, nil, err
	}

	commitMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return "", nil, nil, err
	}

	return curBranchName, commit, commitMeta, nil
}

func stashQuery(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) (sql.RowIter, *sql.Context, func(), error) {
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, generateStashSql(apr, subcommand))
	if err != nil {
		return nil, nil, nil, err
	}

	return rowIter, sqlCtx, closeFunc, nil
}

func stashChanges(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) error {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get working root, cause: %s", err.Error())
	}

	hasChanges, err := hasLocalChanges(ctx, dEnv, roots, apr)
	if err != nil {
		return err
	}
	if !hasChanges {
		cli.Println("No local changes to save")
		return nil
	}

	roots, err = actions.StageModifiedAndDeletedTables(ctx, roots)
	if err != nil {
		return err
	}

	// all tables with changes that are going to be stashed are staged at this point

	allTblsToBeStashed, addedTblsToStage, err := stashedTableSets(ctx, roots)
	if err != nil {
		return err
	}

	// stage untracked files to include them in the stash,
	// but do not include them in added table set,
	// because they should not be staged when popped.
	if apr.Contains(cli.IncludeUntrackedFlag) || apr.Contains(cli.AllFlag) {
		allTblsToBeStashed, err = doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
		if err != nil {
			return err
		}

		roots, err = actions.StageTables(ctx, roots, allTblsToBeStashed, !apr.Contains(cli.AllFlag))
		if err != nil {
			return err
		}
	}

	curHeadRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return err
	}
	curBranchName := curHeadRef.String()
	commitSpec, err := doltdb.NewCommitSpec(curBranchName)
	if err != nil {
		return err
	}
	optCmt, err := dEnv.DoltDB(ctx).Resolve(ctx, commitSpec, curHeadRef)
	if err != nil {
		return err
	}
	commit, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	commitMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return err
	}

	err = dEnv.DoltDB(ctx).AddStash(ctx, commit, roots.Staged, datas.NewStashMeta(curBranchName, commitMeta.Description, doltdb.FlattenTableNames(addedTblsToStage)), doltdb.DoltCliRef)
	if err != nil {
		return err
	}

	// setting STAGED to current HEAD RootValue resets staged set of changed, so
	// these changes are now in working set of changes, which needs to be checked out
	roots.Staged = roots.Head
	roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, allTblsToBeStashed)
	if err != nil {
		return err
	}

	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		return err
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return err
	}
	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", curBranchName, commitHash.String(), commitMeta.Description))
	return nil
}

// workingSetContainsOnlyUntrackedTables returns true if all changes in working set are untracked files/added tables.
// Untracked files are part of working set changes, but should not be stashed unless staged or --include-untracked flag is used.
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

// stashedTableSets returns array of table names for all tables that are being stashed and added tables in staged.
// These table names are determined from all tables in the staged set of changes as they are being stashed only.
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

func parseStashIndex(apr *argparser.ArgParseResults) (int, error) {
	idx := 0

	if apr.NArg() > 1 {
		stashID := apr.Arg(1)
		var err error

		stashID = strings.TrimSuffix(strings.TrimPrefix(stashID, "stash@{"), "}")
		idx, err = strconv.Atoi(stashID)
		if err != nil {
			return 0, fmt.Errorf("error: %s is not a valid reference", stashID)
		}
	}

	return idx, nil
}
