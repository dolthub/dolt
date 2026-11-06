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

package commands

import (
	"context"
	"fmt"
	"io"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

const (
	filterDbName    = "filterDB"
	branchesFlag    = "branches"
	uncommittedFlag = "apply-to-uncommitted"
)

var filterBranchDocs = cli.CommandDocumentationContent{
	ShortDesc: "Edits the commit history using the provided query",
	LongDesc: `Traverses the commit history to the initial commit starting at the current HEAD commit, or a commit you name. Replays all commits, rewriting the history using the provided SQL queries. Separate multiple queries with semicolons. Use the DELIMITER syntax to define stored procedures, triggers, etc. 

If a {{.LessThan}}commit-spec{{.GreaterThan}} is provided, the traversal will stop when the commit is reached and rewriting will begin at that commit, or will error if the commit is not found.

If the {{.EmphasisLeft}}--branches{{.EmphasisRight}} flag is supplied, filter-branch traverses and rewrites commits for all branches.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied, filter-branch traverses and rewrites commits for all branches and tags.
`,

	Synopsis: []string{
		"[--all] -q {{.LessThan}}queries{{.GreaterThan}} [{{.LessThan}}commit{{.GreaterThan}}]",
	},
}

type FilterBranchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd FilterBranchCmd) Name() string {
	return "filter-branch"
}

// Description returns a description of the command
func (cmd FilterBranchCmd) Description() string {
	return fmt.Sprintf("%s.", filterBranchDocs.ShortDesc)
}

func (cmd FilterBranchCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(filterBranchDocs, ap)
}

func (cmd FilterBranchCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.SupportsFlag(cli.VerboseFlag, "v", "logs more information")
	ap.SupportsFlag(branchesFlag, "b", "filter all branches")
	ap.SupportsFlag(uncommittedFlag, "", "apply changes to uncommitted tables")
	ap.SupportsFlag(cli.AllFlag, "a", "filter all branches and tags")
	ap.SupportsFlag(continueFlag, "c", "log a warning and continue if any errors occur executing statements")
	ap.SupportsString(QueryFlag, "q", "queries", "Queries to run, separated by semicolons. If not provided, queries are read from STDIN.")
	return ap
}

// EventType returns the type of the event to log
func (cmd FilterBranchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_FILTER_BRANCH
}

// Exec executes the command
func (cmd FilterBranchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, filterBranchDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 1 {
		args := strings.Join(apr.Args, ", ")
		verr := errhand.BuildDError("%s takes 0 or 1 args, %d provided: %s", cmd.Name(), apr.NArg(), args).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	queryString := apr.GetValueOrDefault(QueryFlag, "")
	verbose := apr.Contains(cli.VerboseFlag)
	continueOnErr := apr.Contains(continueFlag)

	// If we didn't get a query string, read one from STDIN
	if len(queryString) == 0 {
		queryStringBytes, err := io.ReadAll(cli.InStream)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error reading from stdin").AddCause(err).Build(), usage)
		}
		queryString = string(queryStringBytes)
	}

	nerf, err := getNerf(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// Remove a scratch branch left behind by an interrupted filter-branch, so that it isn't rewritten as a branch.
	err = deleteScratchBranch(ctx, dEnv.DoltDB(ctx))
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	rootReplayer := &workingSetReplayer{
		dEnv:          dEnv,
		queryString:   queryString,
		verbose:       verbose,
		continueOnErr: continueOnErr,
	}

	commitReplayer := &commitReplayer{
		dEnv:          dEnv,
		queryString:   queryString,
		verbose:       verbose,
		continueOnErr: continueOnErr,
	}

	applyUncommitted := apr.Contains(uncommittedFlag)
	switch {
	case apr.Contains(branchesFlag):
		err = rebase.AllBranches(ctx, dEnv, applyUncommitted, commitReplayer, rootReplayer, nerf)
	case apr.Contains(cli.AllFlag):
		err = rebase.AllBranchesAndTags(ctx, dEnv, applyUncommitted, commitReplayer, rootReplayer, nerf)
	default:
		err = rebase.CurrentBranch(ctx, dEnv, applyUncommitted, commitReplayer, rootReplayer, nerf)
	}
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

// workingSetReplayer replays working set root values, rebasing them with a specific query, and returns the updated root value
type workingSetReplayer struct {
	dEnv          *env.DoltEnv
	queryString   string
	verbose       bool
	continueOnErr bool
}

var _ rebase.RootReplayer = &workingSetReplayer{}

// ReplayRoot implements the RootReplayer interface
func (r *workingSetReplayer) ReplayRoot(ctx context.Context, root, _, _ doltdb.RootValue) (doltdb.RootValue, error) {
	rootHash, err := root.HashOf()
	if err != nil {
		return nil, err
	}
	rootHashStr := rootHash.String()
	if r.verbose {
		cli.Printf("processing commit %s\n", rootHashStr)
	}

	head, err := r.dEnv.HeadCommit(ctx)
	if err != nil {
		return nil, err
	}

	updatedRoot, err := processFilterQuery(ctx, r.dEnv, head, root, rootHashStr, r.queryString, r.verbose, r.continueOnErr)
	if err != nil {
		return nil, err
	}

	if r.verbose {
		var before, after hash.Hash
		before, err = root.HashOf()
		if err != nil {
			return nil, err
		}
		after, err = updatedRoot.HashOf()
		if err != nil {
			return nil, err
		}
		if before != after {
			cli.Printf("updated commit %s (root: %s -> %s)\n", rootHashStr, before.String(), after.String())
		} else {
			cli.Printf("no changes to commit %s", rootHashStr)
		}
	}
	return updatedRoot, nil
}

// commitReplayer replays a specific commits, rebasing it with a specific query, and returns the updated root value
type commitReplayer struct {
	dEnv          *env.DoltEnv
	queryString   string
	verbose       bool
	continueOnErr bool
}

var _ rebase.CommitReplayer = &commitReplayer{}

// ReplayCommit implements the CommitReplayer interface
func (c *commitReplayer) ReplayCommit(ctx context.Context, commit, _, _ *doltdb.Commit) (doltdb.RootValue, error) {
	root, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	cmHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	cmHashStr := cmHash.String()
	if c.verbose {
		cli.Printf("processing commit %s\n", cmHashStr)
	}

	updatedRoot, err := processFilterQuery(ctx, c.dEnv, commit, root, cmHashStr, c.queryString, c.verbose, c.continueOnErr)
	if err != nil {
		return nil, err
	}

	if c.verbose {
		var before, after hash.Hash
		before, err = root.HashOf()
		if err != nil {
			return nil, err
		}
		after, err = updatedRoot.HashOf()
		if err != nil {
			return nil, err
		}
		if before != after {
			cli.Printf("updated commit %s (root: %s -> %s)\n", cmHashStr, before.String(), after.String())
		} else {
			cli.Printf("no changes to commit %s", cmHashStr)
		}
	}
	return updatedRoot, nil
}

func getNerf(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (rebase.NeedsRebaseFn, error) {
	if apr.NArg() == 0 {
		return rebase.EntireHistory(), nil
	}

	cs, err := doltdb.NewCommitSpec(apr.Arg(0))
	if err != nil {
		return nil, err
	}

	headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return nil, err
	}

	optCmt, err := dEnv.DoltDB(ctx).Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	return rebase.StopAtCommit(cm), nil
}

func processFilterQuery(ctx context.Context, dEnv *env.DoltEnv, head *doltdb.Commit, root doltdb.RootValue, cmHashStr string, query string, verbose bool, continueOnErr bool) (_ doltdb.RootValue, err error) {
	ddb := dEnv.DoltDB(ctx)
	err = resetScratchBranch(ctx, ddb, head, root)
	if err != nil {
		return nil, err
	}
	defer func() {
		deleteErr := deleteScratchBranch(ctx, ddb)
		if err == nil {
			err = deleteErr
		}
	}()

	sqlCtx, eng, err := rebaseSqlEngine(ctx, dEnv)
	if err != nil {
		return nil, err
	}
	// Closing the database stops its sequence tracker's background reads of the store.
	defer eng.Close()

	scanner := NewStreamScanner(strings.NewReader(query))
	for scanner.Scan() {
		q := scanner.Text()

		if verbose {
			cli.Printf("executing query: %s\n", q)
		}

		err = runFilterQuery(sqlCtx, eng, q)
		if err != nil {
			if continueOnErr {
				if verbose {
					cli.PrintErrf("error encountered processing commit %s (continuing): %s\n", cmHashStr, err.Error())
				}
			} else {
				return nil, err
			}
		}
	}

	// Autocommit is off, so the queries' changes reach the scratch branch's working set when this transaction commits.
	err = runFilterQuery(sqlCtx, eng, "COMMIT")
	if err != nil {
		return nil, err
	}

	ws, err := ddb.ResolveWorkingSet(ctx, filterScratchWorkingSetRef())
	if err != nil {
		return nil, err
	}

	return ws.WorkingRoot(), nil
}

// runFilterQuery executes |query| and drains its results.
func runFilterQuery(sqlCtx *sql.Context, eng *filterEngine, query string) error {
	_, itr, _, err := eng.Query(sqlCtx, query)
	if err != nil {
		return err
	}

	for {
		_, err = itr.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			itr.Close(sqlCtx)
			return err
		}
	}

	return itr.Close(sqlCtx)
}

// filterScratchBranch is the branch whose working set filter-branch queries transform. Like the branches created by
// dolt_rebase, it is left behind if filter-branch is interrupted, and is replaced by the next filter-branch.
const filterScratchBranch = "dolt_filter_branch"

func filterScratchWorkingSetRef() ref.WorkingSetRef {
	wsRef, _ := ref.WorkingSetRefForHead(ref.NewBranchRef(filterScratchBranch))
	return wsRef
}

// resetScratchBranch points the scratch branch at |head|, and sets its working and staged roots to |root|.
func resetScratchBranch(ctx context.Context, ddb *doltdb.DoltDB, head *doltdb.Commit, root doltdb.RootValue) error {
	err := ddb.NewBranchAtCommit(ctx, ref.NewBranchRef(filterScratchBranch), head, nil)
	if err != nil {
		return err
	}

	wsRef := filterScratchWorkingSetRef()
	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	prevHash, err := ws.HashOf()
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(root).WithStagedRoot(root)
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, prevHash, ws.Meta(), nil)
}

// deleteScratchBranch deletes the scratch branch and its working set, if they exist.
func deleteScratchBranch(ctx context.Context, ddb *doltdb.DoltDB) error {
	branchRef := ref.NewBranchRef(filterScratchBranch)
	exists, err := ddb.HasRef(ctx, branchRef)
	if err != nil || !exists {
		return err
	}

	err = ddb.DeleteWorkingSet(ctx, filterScratchWorkingSetRef())
	if err != nil {
		return err
	}

	return ddb.DeleteBranch(ctx, branchRef, nil)
}

// rebaseSqlEngine returns an engine and context whose session runs queries against the scratch branch.
// Some functionality will not work on this kind of engine, e.g. many DOLT_ functions.
func rebaseSqlEngine(ctx context.Context, dEnv *env.DoltEnv) (_ *sql.Context, _ *filterEngine, err error) {
	db, err := dsqle.NewDatabase(ctx, filterDbName, dEnv.DbData(ctx), editor.Options{})
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.FS, dEnv)
	if err != nil {
		return nil, nil, err
	}

	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := dsqle.NewDoltDatabaseProviderWithDatabase(b, mrEnv.FileSystem(), db, dEnv.FS, sql.EngineOverrides{})
	if err != nil {
		return nil, nil, err
	}

	vars := dsqle.NewEngineVariables()
	sess := dsess.DefaultSessionWithVariables(pro, writer.NewWriteSession, vars)

	sqlCtx := sql.NewContext(ctx, sql.WithSession(sess))
	err = sqlCtx.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, nil, err
	}

	sqlCtx.SetCurrentDatabase(filterDbName + doltdb.DbRevisionDelimiter + filterScratchBranch)

	eng := &filterEngine{
		Engine: sqle.New(analyzer.NewDefault(pro), &sqle.Config{IsReadOnly: false, EngineVariables: vars}),
		db:     db,
	}
	return sqlCtx, eng, nil
}

// filterEngine is the engine filter-branch queries run on.
type filterEngine struct {
	*sqle.Engine
	// db is owned by the engine and closed in Close
	db dsess.SqlDatabase
}

// Close closes the engine and the database it was built over.
func (e *filterEngine) Close() error {
	err := e.Engine.Close()
	e.db.Close()
	return err
}
