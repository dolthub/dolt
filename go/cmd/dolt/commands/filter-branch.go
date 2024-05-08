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
	"runtime"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	filterDbName = "filterDB"
	branchesFlag = "branches"
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

	replay := func(ctx context.Context, commit, _, _ *doltdb.Commit) (doltdb.RootValue, error) {
		var cmHash, before hash.Hash
		var root doltdb.RootValue
		if verbose {
			var err error
			cmHash, err = commit.HashOf()
			if err != nil {
				return nil, err
			}

			cli.Printf("processing commit %s\n", cmHash.String())

			root, err = commit.GetRootValue(ctx)
			if err != nil {
				return nil, err
			}
			before, err = root.HashOf()
			if err != nil {
				return nil, err
			}
		}

		updatedRoot, err := processFilterQuery(ctx, dEnv, commit, queryString, verbose, continueOnErr)

		if err != nil {
			return nil, err
		}

		if verbose {
			after, err := updatedRoot.HashOf()
			if err != nil {
				return nil, err
			}
			if before != after {
				cli.Printf("updated commit %s (root: %s -> %s)\n",
					cmHash.String(), before.String(), after.String())
			} else {
				cli.Printf("no changes to commit %s", cmHash.String())
			}
		}
		return updatedRoot, nil
	}

	nerf, err := getNerf(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	switch {
	case apr.Contains(branchesFlag):
		err = rebase.AllBranches(ctx, dEnv, replay, nerf)
	case apr.Contains(cli.AllFlag):
		err = rebase.AllBranchesAndTags(ctx, dEnv, replay, nerf)
	default:
		err = rebase.CurrentBranch(ctx, dEnv, replay, nerf)
	}
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

func getNerf(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (rebase.NeedsRebaseFn, error) {
	if apr.NArg() == 0 {
		return rebase.EntireHistory(), nil
	}

	cs, err := doltdb.NewCommitSpec(apr.Arg(0))
	if err != nil {
		return nil, err
	}

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return nil, err
	}

	optCmt, err := dEnv.DoltDB.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	return rebase.StopAtCommit(cm), nil
}

func processFilterQuery(ctx context.Context, dEnv *env.DoltEnv, cm *doltdb.Commit, query string, verbose bool, continueOnErr bool) (doltdb.RootValue, error) {
	sqlCtx, eng, err := rebaseSqlEngine(ctx, dEnv, cm)
	if err != nil {
		return nil, err
	}

	scanner := NewSqlStatementScanner(strings.NewReader(query))
	if err != nil {
		return nil, err
	}

	for scanner.Scan() {
		q := scanner.Text()

		if verbose {
			cli.Printf("executing query: %s\n", q)
		}

		err = func() error {
			_, itr, err := eng.Query(sqlCtx, q)
			if err != nil {
				return err
			}

			for {
				_, err = itr.Next(sqlCtx)
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
			return itr.Close(sqlCtx)
		}()

		if err != nil {
			if continueOnErr {
				if verbose {
					cmHash, cmErr := cm.HashOf()
					if cmErr != nil {
						return nil, err
					}
					cli.PrintErrf("error encountered processing commit %s (continuing): %s\n", cmHash.String(), err.Error())
				}
			} else {
				return nil, err
			}
		}
	}

	sess := dsess.DSessFromSess(sqlCtx.Session)
	ws, err := sess.WorkingSet(sqlCtx, filterDbName)
	if err != nil {
		return nil, err
	}

	return ws.WorkingRoot(), nil
}

// rebaseSqlEngine packages up the context necessary to run sql queries against single root
// The SQL engine returned has transactions disabled. This is to prevent transactions starts from overwriting the root
// we set manually with the one at the working set of the HEAD being rebased.
// Some functionality will not work on this kind of engine, e.g. many DOLT_ functions.
func rebaseSqlEngine(ctx context.Context, dEnv *env.DoltEnv, cm *doltdb.Commit) (*sql.Context, *engine.SqlEngine, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, nil, err
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := dsqle.NewDatabase(ctx, filterDbName, dEnv.DbData(), opts)
	if err != nil {
		return nil, nil, err
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return nil, nil, err
	}

	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := dsqle.NewDoltDatabaseProviderWithDatabase(b, mrEnv.FileSystem(), db, dEnv.FS)
	if err != nil {
		return nil, nil, err
	}

	sess := dsess.DefaultSession(pro)

	sqlCtx := sql.NewContext(ctx, sql.WithSession(sess))
	err = sqlCtx.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, nil, err
	}

	err = sqlCtx.SetSessionVariable(sqlCtx, dsess.TransactionsDisabledSysVar, true)
	if err != nil {
		return nil, nil, err
	}

	parallelism := runtime.GOMAXPROCS(0)
	azr := analyzer.NewBuilder(pro).WithParallelism(parallelism).Build()

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	err = db.SetRoot(sqlCtx, root)
	if err != nil {
		return nil, nil, err
	}

	sqlCtx.SetCurrentDatabase(filterDbName)

	se := engine.NewRebasedSqlEngine(sqle.New(azr, &sqle.Config{IsReadOnly: false}), map[string]dsess.SqlDatabase{filterDbName: db})

	return sqlCtx, se, nil
}
