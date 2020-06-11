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
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff/querydiff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

//var diffDocs = cli.CommandDocumentationContent{
var queryDiffDocs = cli.CommandDocumentationContent{
	ShortDesc: "",
	LongDesc:  "",
	Synopsis:  nil,
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

func (cmd QueryDiffCmd) Hidden() bool {
	return true
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
	if len(leftover) < 1 {
		verr = errhand.BuildDError("too many arguments: %s", strings.Join(apr.Args(), " ")).Build()
	} else if len(leftover) > 1 {
		verr = errhand.BuildDError("too many arguments: %s", strings.Join(apr.Args(), " ")).Build()
	}
	if verr != nil {
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

func validateQueryDiff(ctx context.Context, dEnv *env.DoltEnv, from *doltdb.RootValue, to *doltdb.RootValue, query string) errhand.VerboseError {
	//sqlCtx, eng, err := makeSqlEngine(ctx, dEnv, to)
	//if err != nil {
	//	return errhand.BuildDError("Cannot diff query, query is not ordered. Error describing query plan").AddCause(err).Build()
	//}
	//
	//query = fmt.Sprintf("describe %s", query)
	//_, iter, err := processQuery(sqlCtx, query, eng)
	//if err != nil {
	//	return errhand.BuildDError("Cannot diff query, query is not ordered. Error describing query plan").AddCause(err).Build()
	//}
	//
	//var qp strings.Builder
	//for {
	//	r, err := iter.Next()
	//	if err == io.EOF {
	//		break
	//	} else if err != nil {
	//		return errhand.BuildDError("Cannot diff query, query is not ordered. Error describing query plan").AddCause(err).Build()
	//	}
	//	sv, _ := typeinfo.StringDefaultType.ConvertValueToNomsValue(r[0])
	//	qp.WriteString(fmt.Sprintf("%s\n", string(sv.(types.String))))
	//}
	//
	//return errhand.BuildDError("Cannot diff query, query is not ordered. Add ORDER BY statement.\nquery plan:\n%s", qp.String()).Build()
	return nil
}

func diffQuery(ctx context.Context, dEnv *env.DoltEnv, fromRoot, toRoot *doltdb.RootValue, query string) errhand.VerboseError {
	qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, query)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	doltSch := doltSchWithPKFromSqlSchema(qd.Schema())

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: diff.From, Sch: doltSch},
			{Name: diff.To, Sch: doltSch},
		},
		map[string]rowconv.ColNamingFunc{diff.To: toNamer, diff.From: fromNamer},
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	p, err := buildQueryDiffPipeline(qd, doltSch, joiner)

	if err != nil {
		return errhand.BuildDError("error building diff pipeline").AddCause(err).Build()
	}

	p.Start()

	return errhand.VerboseErrorFromError(p.Wait())
}

func doltSchWithPKFromSqlSchema(sch sql.Schema) schema.Schema {
	dSch, _ := dsqle.SqlSchemaToDoltResultSchema(sch)
	// make the first col the PK
	pk := false
	newCC, _ := schema.MapColCollection(dSch.GetAllCols(), func(col schema.Column) (column schema.Column, err error) {
		if !pk {
			col.IsPartOfPK = true
			pk = true
		}
		return col, nil
	})
	return schema.SchemaFromCols(newCC)
}

func nextQueryDiff(qd *querydiff.QueryDiffer, joiner *rowconv.Joiner) (row.Row, pipeline.ImmutableProperties, error) {
	fromRow, toRow, err := qd.NextDiff()
	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	rows := make(map[string]row.Row)
	if fromRow != nil {
		sch := joiner.SchemaForName(diff.From)
		oldRow, err := dsqle.SqlRowToDoltRow(types.Format_Default, fromRow, sch)
		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}
		rows[diff.From] = oldRow
	}

	if toRow != nil {
		sch := joiner.SchemaForName(diff.To)
		newRow, err := dsqle.SqlRowToDoltRow(types.Format_Default, toRow, sch)
		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}
		rows[diff.To] = newRow
	}

	joinedRow, err := joiner.Join(rows)
	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	return joinedRow, pipeline.ImmutableProperties{}, nil
}

func buildQueryDiffPipeline(qd *querydiff.QueryDiffer, doltSch schema.Schema, joiner *rowconv.Joiner) (*pipeline.Pipeline, error) {

	unionSch, ds, verr := createSplitter(doltSch, doltSch, joiner, &diffArgs{diffOutput: TabularDiffOutput})
	if verr != nil {
		return nil, verr
	}

	transforms := pipeline.NewTransformCollection()
	nullPrinter := nullprinter.NewNullPrinter(unionSch)
	fwtTr := fwt.NewAutoSizingFWTTransformer(unionSch, fwt.HashFillWhenTooLong, 1000)
	transforms.AppendTransforms(
		pipeline.NewNamedTransform("split_diffs", ds.SplitDiffIntoOldAndNew),
		pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow),
		pipeline.NamedTransform{Name: fwtStageName, Func: fwtTr.TransformToFWT},
	)

	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		verr := errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		cli.PrintErrln(verr.Error())
		return true
	}

	sink, err := diff.NewColorDiffSink(iohelp.NopWrCloser(cli.CliOut), doltSch, 1)
	if err != nil {
		return nil, err
	}
	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)

	srcProcFunc := pipeline.ProcFuncForSourceFunc(func() (row.Row, pipeline.ImmutableProperties, error) {
		return nextQueryDiff(qd, joiner)
	})

	p := pipeline.NewAsyncPipeline(srcProcFunc, sinkProcFunc, transforms, badRowCB)

	names := make(map[uint64]string, doltSch.GetAllCols().Size())
	_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		names[tag] = col.Name
		return false, nil
	})
	schRow, err := untyped.NewRowFromTaggedStrings(types.Format_Default, unionSch, names)
	if err != nil {
		return nil, err
	}
	p.InjectRow(fwtStageName, schRow)

	p.RunAfter(func() {
		err := sink.Close()
		if err != nil {
			cli.PrintErrln(err)
		}
		err = qd.Close()
		if err != nil {
			cli.PrintErrln(err)
		}
	})

	return p, nil
}
