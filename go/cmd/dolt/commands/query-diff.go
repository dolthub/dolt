// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

const (
	fromDB = "from"
	toDB   = "to"
)

//var diffDocs = cli.CommandDocumentationContent{
var queryDiffDocs = cli.CommandDocumentationContent{
	ShortDesc: "",
	LongDesc: "",
	Synopsis: nil,
}

type QueryDiffCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd QueryDiffCmd) Name() string {
	return "query_diff"
}

// Description returns a description of the command
func (cmd QueryDiffCmd) Description() string {
	return "Diffs the results of a query between two roots"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd QueryDiffCmd) RequiresRepo() bool {
	return true
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd QueryDiffCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func (cmd QueryDiffCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd QueryDiffCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, queryDiffDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	from, to, leftover, err := getDiffRoots(ctx, dEnv, apr.Args())

	var verr errhand.VerboseError
	if err != nil {
		verr = errhand.BuildDError("error determining diff commits for args: %s", strings.Join(apr.Args(), " ")).AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}
	if len(leftover) != 1 {
		verr = errhand.BuildDError("too many arguments: %s", strings.Join(apr.Args(), " ")).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	verr = diffQuery(ctx, dEnv, from, to, leftover[0])

	return HandleVErrAndExitCode(verr, usage)
}

func getDiffRoots(ctx context.Context, dEnv *env.DoltEnv, args []string) (from, to *doltdb.RootValue, leftover []string, err error) {
	headRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	//workingRoot, err := dEnv.WorkingRootWithDocs(ctx) // todo: uncomment
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(args) == 0 {
		// `dolt diff`
		from = headRoot
		to = workingRoot
		return from, to, nil, nil
	}

	from, ok := maybeResolve(ctx, dEnv, args[0])

	if !ok {
		// `dolt diff ...tables`
		from = headRoot
		to = workingRoot
		leftover = args
		return from, to, leftover, nil
	}

	if len(args) == 1 {
		// `dolt diff from_commit`
		to = workingRoot
		return from, to, nil, nil
	}

	to, ok = maybeResolve(ctx, dEnv, args[1])

	if !ok {
		// `dolt diff from_commit ...tables`
		to = workingRoot
		leftover = args[1:]
		return from, to, leftover, nil
	}

	// `dolt diff from_commit to_commit ...tables`
	leftover = args[2:]
	return from, to, leftover, nil
}

func maybeResolve(ctx context.Context, dEnv *env.DoltEnv, spec string) (*doltdb.RootValue, bool) {
	cs, err := doltdb.NewCommitSpec(spec, dEnv.RepoState.CWBHeadRef().String())
	if err != nil {
		return nil, false
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)
	if err != nil {
		return nil, false
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return nil, false
	}

	return root, true
}

func diffQuery(ctx context.Context, dEnv *env.DoltEnv, fromRoot, toRoot *doltdb.RootValue, query string) errhand.VerboseError {
	fromIter, toIter, sch, verr := getRowIters(ctx, dEnv, fromRoot, toRoot, query)

	if verr != nil {
		return verr
	}

	ordFromIter, ok1 := fromIter.(sql.OrderableRowIter)
	ordToIter, ok2 := toIter.(sql.OrderableRowIter)
	if !ok1 || !ok2 {
		return errWithQueryPlan(ctx, toRoot, query)
	}

	rowCmp, err := ordFromIter.RowCompareFunc(sch)
	if err != nil {
		return errWithQueryPlan(ctx, toRoot, query)
	}

	p, verr := buildQueryDiffPipeline(ctx, sch, queryDiffer{
		fromIter: ordFromIter,
		toIter: ordToIter,
		rowCmp: rowCmp,
	})

	if verr != nil {
		return verr
	}

	p.Start()

	return errhand.VerboseErrorFromError(p.Wait())
}

func getRowIters(ctx context.Context, dEnv *env.DoltEnv, fromRoot, toRoot *doltdb.RootValue, query string) (from, to sql.RowIter, sch sql.Schema, verr errhand.VerboseError) {
	sqlCtx := sql.NewContext(ctx,
		sql.WithSession(dsqle.DefaultDoltSession()),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry()))
	mrEnv := env.DoltEnvAsMultiEnv(dEnv)

	roots := map[string]*doltdb.RootValue{fromDB: fromRoot, toDB:   toRoot}
	dbs := []dsqle.Database{newDatabase(fromDB, dEnv), newDatabase(toDB, dEnv)}

	sqlCtx.SetCurrentDatabase(fromDB)
	fromEng, err := newSqlEngine(sqlCtx, mrEnv, roots, formatTabular, dbs...)
	if err != nil {
		return nil,  nil, nil, errhand.VerboseErrorFromError(err)
	}

	fromSch, fromIter, err := processQuery(sqlCtx, query, fromEng)
	if err != nil {
		// todo: improve err msg
		return nil,  nil, nil, formatQueryError("cannot execute query at from root", err)
	}

	sqlCtx.SetCurrentDatabase(toDB)
	toEng, err := newSqlEngine(sqlCtx, mrEnv, roots, formatTabular, dbs...)
	if err != nil {
		return nil,  nil, nil, errhand.VerboseErrorFromError(err)
	}

	toSch, toIter, err := processQuery(sqlCtx, query, toEng)
	if err != nil {
		// todo: improve err msg
		return nil,  nil, nil, formatQueryError("cannot execut query at to root", err)
	}

	if !fromSch.Equals(toSch) {
		return nil,  nil, nil, errhand.BuildDError("cannot diff query, result schemas are not equal").Build()
	}

	return fromIter, toIter, toSch, nil
}

// todo: print query plan if we can't diff query
func errWithQueryPlan(ctx context.Context, root *doltdb.RootValue, query string) errhand.VerboseError {
	return errhand.BuildDError("Cannot diff query, query is not ordered. Add order by statement.").Build()
}

type queryDiffer struct {
	fromIter sql.RowIter
	toIter   sql.RowIter
	rowCmp	 sql.RowCompareFunc
}

func (qd queryDiffer) NextDiff() (row.Row, pipeline.ImmutableProperties, error) {
	return nil, pipeline.NoProps, nil
}

func buildQueryDiffPipeline(ctx context.Context, sch sql.Schema, qd queryDiffer) (*pipeline.Pipeline, errhand.VerboseError) {
	return nil, nil
}