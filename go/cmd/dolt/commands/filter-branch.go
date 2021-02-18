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
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/dolthub/vitess/go/vt/vterrors"
	"github.com/fatih/color"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	dbName = "filterDB"
)

var filterBranchDocs = cli.CommandDocumentationContent{
	ShortDesc: "Edits the commit history using the provided query",
	LongDesc: `Traverses the commit history to the initial commit starting at the current HEAD commit. Replays all commits, rewriting the history using the provided SQL query.

If a {{.LessThan}}commit-spec{{.GreaterThan}} is provided, the traversal will stop when the commit is reached and rewriting will begin at that commit, or will error if the commit is not found.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied, the traversal starts with the HEAD commits of all branches.
`,

	Synopsis: []string{
		"[--all] {{.LessThan}}query{{.GreaterThan}} [{{.LessThan}}commit{{.GreaterThan}}]",
	},
}

type missingTbls map[hash.Hash]*errors.Error

type FilterBranchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd FilterBranchCmd) Name() string {
	return "filter-branch"
}

// Description returns a description of the command
func (cmd FilterBranchCmd) Description() string {
	return fmt.Sprintf("%s.", filterBranchDocs.ShortDesc)
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd FilterBranchCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, filterBranchDocs, ap))
}

func (cmd FilterBranchCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(allFlag, "a", "filter all branches")
	return ap
}

// EventType returns the type of the event to log
func (cmd FilterBranchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_FILTER_BRANCH
}

// Exec executes the command
func (cmd FilterBranchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, filterBranchDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 1 || apr.NArg() > 2 {
		args := strings.Join(apr.Args(), ", ")
		verr := errhand.BuildDError("%s takes 1 or 2 args, %d provided: %s", cmd.Name(), apr.NArg(), args).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	query := apr.Arg(0)
	notFound := make(missingTbls)
	replay := func(ctx context.Context, commit, _, _ *doltdb.Commit) (*doltdb.RootValue, error) {
		return processFilterQuery(ctx, dEnv, commit, query, notFound)
	}

	nerf, err := getNerf(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.Contains(allFlag) {
		err = rebase.AllBranches(ctx, dEnv, replay, nerf)
	} else {
		err = rebase.CurrentBranch(ctx, dEnv, replay, nerf)
	}
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	for h, e := range notFound {
		cli.PrintErrln(color.YellowString("for root value %s: %s", h.String(), e.Error()))
	}

	return 0
}

func getNerf(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (rebase.NeedsRebaseFn, error) {
	if apr.NArg() == 1 {
		return rebase.EntireHistory(), nil
	}

	cs, err := doltdb.NewCommitSpec(apr.Arg(1))
	if err != nil {
		return nil, err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	return rebase.StopAtCommit(cm), nil
}

func processFilterQuery(ctx context.Context, dEnv *env.DoltEnv, cm *doltdb.Commit, query string, mt missingTbls) (*doltdb.RootValue, error) {
	root, err := cm.GetRootValue()
	if err != nil {
		return nil, err
	}

	sqlCtx, eng, err := monoSqlEngine(ctx, dEnv, cm)
	if err != nil {
		return nil, err
	}

	rh, err := root.HashOf()
	if err != nil {
		return nil, err
	}

	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	itr := sql.RowsToRowIter() // empty RowIter
	switch s := sqlStatement.(type) {
	case *sqlparser.Insert, *sqlparser.Update:
		_, itr, err = eng.query(sqlCtx, query)

	case *sqlparser.Delete:
		_, itr, err = eng.query(sqlCtx, query)

	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if se, ok := vterrors.AsSyntaxError(err); ok {
			return nil, vterrors.SyntaxError{Message: "While Parsing DDL: " + se.Message, Position: se.Position, Statement: se.Statement}
		}
		if err != nil {
			return nil, fmt.Errorf("error parsing DDL: %v", err.Error())
		}
		// ddl returns a nil itr
		_, _, err = eng.ddl(sqlCtx, s, query)

	case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
		return nil, fmt.Errorf("filter-branch queries must be write queries: '%s'", query)

	default:
		return nil, fmt.Errorf("SQL statement not supported for filter-branch: '%s'", query)
	}

	err, ok := captureTblNotFoundErr(err, mt, rh)
	if ok {
		// table doesn't exist, save the error and continue
		return root, nil
	}
	if err != nil {
		return nil, err
	}

	for {
		_, err = itr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
	}
	err = itr.Close(sqlCtx)
	if err != nil {
		return nil, err
	}

	roots, err := eng.getRoots(sqlCtx)
	if err != nil {
		return nil, err
	}

	return roots[dbName], nil
}

// monoSqlEngine packages up the context necessary to run sql queries against single root.
func monoSqlEngine(ctx context.Context, dEnv *env.DoltEnv, cm *doltdb.Commit) (*sql.Context, *sqlEngine, error) {
	dsess := dsqle.DefaultDoltSession()

	sqlCtx := sql.NewContext(ctx,
		sql.WithSession(dsess),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry()),
		sql.WithTracer(tracing.Tracer(ctx)))
	_ = sqlCtx.Set(sqlCtx, sql.AutoCommitSessionVar, sql.Boolean, true)

	db := dsqle.NewDatabase(dbName, dEnv.DbData())

	cat := sql.NewCatalog()
	err := cat.Register(dfunctions.DoltFunctions...)
	if err != nil {
		return nil, nil, err
	}

	parallelism := runtime.GOMAXPROCS(0)
	azr := analyzer.NewBuilder(cat).WithParallelism(parallelism).Build()

	engine := sqle.New(cat, azr, &sqle.Config{Auth: new(auth.None)})
	engine.AddDatabase(db)

	err = dsess.AddDB(sqlCtx, db)
	if err != nil {
		return nil, nil, err
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	err = db.SetRoot(sqlCtx, root)
	if err != nil {
		return nil, nil, err
	}

	err = dsqle.RegisterSchemaFragments(sqlCtx, db, root)
	if err != nil {
		return nil, nil, err
	}

	sqlCtx.SetCurrentDatabase(dbName)

	se := &sqlEngine{
		dbs:    map[string]dsqle.Database{dbName: db},
		mrEnv:  env.MultiRepoEnv{dbName: dEnv},
		engine: engine,
	}

	return sqlCtx, se, nil
}

func captureTblNotFoundErr(e error, mt missingTbls, h hash.Hash) (error, bool) {
	if sql.ErrTableNotFound.Is(e) {
		mt[h] = e.(*errors.Error)
		return nil, true
	}
	return e, false
}
