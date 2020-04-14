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
	"fmt"
	"reflect"
	"strconv"
	"strings"

	textdiff "github.com/andreyvit/diff"
	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/mathutil"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type diffOutput int
type diffPart int

const (
	SchemaOnlyDiff diffPart = 1 // 0b0001
	DataOnlyDiff   diffPart = 2 // 0b0010
	Summary        diffPart = 4 // 0b0100

	SchemaAndDataDiff = SchemaOnlyDiff | DataOnlyDiff

	TabularDiffOutput diffOutput = 1
	SQLDiffOutput     diffOutput = 2

	DataFlag    = "data"
	SchemaFlag  = "schema"
	SummaryFlag = "summary"
	whereParam  = "where"
	limitParam  = "limit"
	SQLFlag     = "sql"
)

type DiffSink interface {
	GetSchema() schema.Schema
	ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error
	Close() error
}

var diffDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show changes between commits, commit and working tree, etc",
	LongDesc: `
Show changes between the working and staged tables, changes between the working tables and the tables within a commit, or changes between tables at two commits.

{{.EmphasisLeft}}dolt diff [--options] [<tables>...]{{.EmphasisRight}}
   This form is to view the changes you made relative to the staging area for the next commit. In other words, the differences are what you could tell Dolt to further add but you still haven't. You can stage these changes by using dolt add.

{{.EmphasisLeft}}dolt diff [--options] <commit> [<tables>...]{{.EmphasisRight}}
   This form is to view the changes you have in your working tables relative to the named {{.LessThan}}commit{{.GreaterThan}}. You can use HEAD to compare it with the latest commit, or a branch name to compare with the tip of a different branch.

{{.EmphasisLeft}}dolt diff [--options] <commit> <commit> [<tables>...]{{.EmphasisRight}}
   This is to view the changes between two arbitrary {{.EmphasisLeft}}commit{{.EmphasisRight}}.

The diffs displayed can be limited to show the first N by providing the parameter {{.EmphasisLeft}}--limit N{{.EmphasisRight}} where {{.EmphasisLeft}}N{{.EmphasisRight}} is the number of diffs to display.

In order to filter which diffs are displayed {{.EmphasisLeft}}--where key=value{{.EmphasisRight}} can be used.  The key in this case would be either {{.EmphasisLeft}}to_COLUMN_NAME{{.EmphasisRight}} or {{.EmphasisLeft}}from_COLUMN_NAME{{.EmphasisRight}}. where {{.EmphasisLeft}}from_COLUMN_NAME=value{{.EmphasisRight}} would filter based on the original value and {{.EmphasisLeft}}to_COLUMN_NAME{{.EmphasisRight}} would select based on its updated value.
`,
	Synopsis: []string{
		`[options] [{{.LessThan}}commit{{.GreaterThan}}] [{{.LessThan}}tables{{.GreaterThan}}...]`,
		`[options] {{.LessThan}}commit{{.GreaterThan}} {{.LessThan}}commit{{.GreaterThan}} [{{.LessThan}}tables{{.GreaterThan}}...]`,
	},
}

type diffArgs struct {
	diffParts  diffPart
	diffOutput diffOutput
	limit      int
	where      string
}

type DiffCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DiffCmd) Name() string {
	return "diff"
}

// Description returns a description of the command
func (cmd DiffCmd) Description() string {
	return "Diff a table."
}

// EventType returns the type of the event to log
func (cmd DiffCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DIFF
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd DiffCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, diffDocs, ap))
}

func (cmd DiffCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(DataFlag, "d", "Show only the data changes, do not show the schema changes (Both shown by default).")
	ap.SupportsFlag(SchemaFlag, "s", "Show only the schema changes, do not show the data changes (Both shown by default).")
	ap.SupportsFlag(SummaryFlag, "", "Show summary of data changes")
	ap.SupportsFlag(SQLFlag, "q", "Output diff as a SQL patch file of {{.EmphasisLeft}}INSERT{{.EmphasisRight}} / {{.EmphasisLeft}}UPDATE{{.EmphasisRight}} / {{.EmphasisLeft}}DELETE{{.EmphasisRight}} statements")
	ap.SupportsString(whereParam, "", "column", "filters columns based on values in the diff.  See {{.EmphasisLeft}}dolt diff --help{{.EmphasisRight}} for details.")
	ap.SupportsInt(limitParam, "", "record_count", "limits to the first N diffs.")
	return ap
}

// Exec executes the command
func (cmd DiffCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, diffDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	diffParts := SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		diffParts = SchemaOnlyDiff
	}

	diffOutput := TabularDiffOutput
	if apr.Contains(SQLFlag) {
		diffOutput = SQLDiffOutput
	}

	summary := apr.Contains(SummaryFlag)

	if summary {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) {
			cli.PrintErrln("Invalid Arguments: --summary cannot be combined with --schema or --data")
			return 1
		}

		diffParts = Summary
	}

	if apr.ContainsArg(doltdb.DocTableName) {
		return HandleDocTableVErrAndExitCode()
	}

	r1, r2, tables, docs, verr := getRoots(ctx, apr.Args(), dEnv)

	// default value of 0 used to signal no limit.
	limit, _ := apr.GetInt(limitParam)

	if verr == nil {
		whereClause := apr.GetValueOrDefault(whereParam, "")

		verr = diffRoots(ctx, r1, r2, tables, docs, dEnv, &diffArgs{diffParts, diffOutput, limit, whereClause})
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

// this doesnt work correctly.  Need to be able to distinguish commits from tables
func getRoots(ctx context.Context, args []string, dEnv *env.DoltEnv) (r1, r2 *doltdb.RootValue, tables []string, docs []doltdb.DocDetails, verr errhand.VerboseError) {
	roots := make([]*doltdb.RootValue, 2)

	i := 0
	for _, arg := range args {
		cs, err := doltdb.NewCommitSpec(arg, dEnv.RepoState.CWBHeadRef().String())
		if err != nil {
			break
		}

		cm, err := dEnv.DoltDB.Resolve(ctx, cs)
		if err != nil {
			break
		}

		roots[i], err = cm.GetRootValue()

		if err != nil {
			return nil, nil, nil, nil, errhand.BuildDError("error: failed to get root").AddCause(err).Build()
		}

		i++
	}

	args, docDetails, err := actions.GetTblsAndDocDetails(dEnv, args)
	if err != nil {
		return nil, nil, nil, nil, errhand.BuildDError("error: failed to read args").AddCause(err).Build()
	}

	if i < 2 {
		roots[1] = roots[0]
		wrkRoot, verr := GetWorkingWithVErr(dEnv)
		if verr == nil && i == 0 {
			roots[1], verr = GetStagedWithVErr(dEnv)
		}
		wrkRootWithDocs, err := dEnv.GetUpdatedRootWithDocs(ctx, wrkRoot, docDetails)
		if err != nil {
			return nil, nil, nil, nil, errhand.BuildDError("error: failed to get docs").AddCause(err).Build()
		}

		roots[0] = wrkRootWithDocs

		if verr != nil {
			return nil, nil, args, nil, verr
		}
	}

	for ; i < len(args); i++ {
		tbl := args[i]

		has0, err := roots[0].HasTable(ctx, tbl)

		if err != nil {
			return nil, nil, nil, nil, errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		has1, err := roots[1].HasTable(ctx, tbl)

		if err != nil {
			return nil, nil, nil, nil, errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		if !(has0 || has1) {
			verr := errhand.BuildDError("error: Unknown table: '%s'", tbl).Build()
			return nil, nil, nil, nil, verr
		}

		tables = append(tables, tbl)
	}

	return roots[0], roots[1], tables, docDetails, nil
}

func getRootForCommitSpecStr(ctx context.Context, csStr string, dEnv *env.DoltEnv) (string, *doltdb.RootValue, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(csStr, dEnv.RepoState.CWBHeadRef().String())

	if err != nil {
		bdr := errhand.BuildDError(`"%s" is not a validly formatted branch, or commit reference.`, csStr)
		return "", nil, bdr.AddCause(err).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r, err := cm.GetRootValue()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get root").AddCause(err).Build()
	}

	h, err := cm.HashOf()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get commit hash").AddCause(err).Build()
	}

	return h.String(), r, nil
}

func diffRoots(ctx context.Context, r1, r2 *doltdb.RootValue, tblNames []string, docDetails []doltdb.DocDetails, dEnv *env.DoltEnv, dArgs *diffArgs) errhand.VerboseError {
	var err error
	if len(tblNames) == 0 {
		tblNames, err = doltdb.UnionTableNames(ctx, r1, r2)
	}

	if err != nil {
		return errhand.BuildDError("error: unable to read tables").AddCause(err).Build()
	}

	if dArgs.diffOutput == SQLDiffOutput {
		err = diff.PrintSqlTableDiffs(ctx, r1, r2, iohelp.NopWrCloser(cli.CliOut))

		if err != nil {
			return errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
		}
	}

	for _, tblName := range tblNames {
		tbl1, ok1, err := r1.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
		}

		tbl2, ok2, err := r2.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
		}

		if !ok1 && !ok2 {
			bdr := errhand.BuildDError("Table could not be found.")
			bdr.AddDetails("The table %s does not exist.", tblName)
			cli.PrintErrln(bdr.Build())
		} else if tbl1 != nil && tbl2 != nil {
			h1, err := tbl1.HashOf()

			if err != nil {
				return errhand.BuildDError("error: failed to get table hash").Build()
			}

			h2, err := tbl2.HashOf()

			if err != nil {
				return errhand.BuildDError("error: failed to get table hash").Build()
			}

			if h1 == h2 {
				continue
			}
		}

		if dArgs.diffOutput == TabularDiffOutput {
			printTableDiffSummary(ctx, dEnv, tblName, tbl1, tbl2, docDetails)
		}

		if tbl1 == nil || tbl2 == nil || tblName == doltdb.DocTableName {
			continue
		}

		var sch1 schema.Schema
		var sch2 schema.Schema
		var sch1Hash hash.Hash
		var sch2Hash hash.Hash
		rowData1, err := types.NewMap(ctx, dEnv.DoltDB.ValueReadWriter())

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}

		rowData2, err := types.NewMap(ctx, dEnv.DoltDB.ValueReadWriter())

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}

		if ok1 {
			sch1, err = tbl1.GetSchema(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
			}

			schRef, err := tbl1.GetSchemaRef()

			if err != nil {
				return errhand.BuildDError("error: failed to get schema ref").AddCause(err).Build()
			}

			sch1Hash = schRef.TargetHash()
			rowData1, err = tbl1.GetRowData(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
			}
		}

		if ok2 {
			sch2, err = tbl2.GetSchema(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
			}

			schRef, err := tbl2.GetSchemaRef()

			if err != nil {
				return errhand.BuildDError("error: failed to get schema ref").AddCause(err).Build()
			}

			sch2Hash = schRef.TargetHash()
			rowData2, err = tbl2.GetRowData(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
			}
		}

		var verr errhand.VerboseError

		if dArgs.diffParts&Summary != 0 {
			colLen := sch2.GetAllCols().Size()
			verr = diffSummary(ctx, rowData1, rowData2, colLen)
		}

		if dArgs.diffParts&SchemaOnlyDiff != 0 && sch1Hash != sch2Hash {
			verr = diffSchemas(tblName, sch2, sch1, dArgs)
		}

		if dArgs.diffParts&DataOnlyDiff != 0 {
			verr = diffRows(ctx, rowData1, rowData2, sch1, sch2, dArgs, tblName)
		}

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffSchemas(tableName string, sch1 schema.Schema, sch2 schema.Schema, dArgs *diffArgs) errhand.VerboseError {
	diffs, unionTags := diff.DiffSchemas(sch1, sch2)

	if dArgs.diffOutput == TabularDiffOutput {
		if verr := tabularSchemaDiff(tableName, unionTags, diffs); verr != nil {
			return verr
		}
	} else {
		sqlSchemaDiff(tableName, unionTags, diffs)
	}

	return nil
}

func tabularSchemaDiff(tableName string, tags []uint64, diffs map[uint64]diff.SchemaDifference) errhand.VerboseError {
	cli.Println("  CREATE TABLE", tableName, "(")

	oldPks := make([]string, 0)
	newPks := make([]string, 0)

	for _, tag := range tags {
		dff := diffs[tag]
		switch dff.DiffType {
		case diff.SchDiffNone:
			if dff.New.IsPartOfPK {
				newPks = append(newPks, sql.QuoteIdentifier(dff.New.Name))
				oldPks = append(oldPks, sql.QuoteIdentifier(dff.Old.Name))
			}
			cli.Println(sql.FmtCol(4, 0, 0, *dff.New))
		case diff.SchDiffColAdded:
			if dff.New.IsPartOfPK {
				newPks = append(newPks, sql.QuoteIdentifier(dff.New.Name))
			}
			cli.Println(color.GreenString("+ " + sql.FmtCol(2, 0, 0, *dff.New)))
		case diff.SchDiffColRemoved:
			// removed from sch2
			if dff.Old.IsPartOfPK {
				oldPks = append(oldPks, sql.QuoteIdentifier(dff.Old.Name))
			}
			cli.Println(color.RedString("- " + sql.FmtCol(2, 0, 0, *dff.Old)))
		case diff.SchDiffColModified:
			// changed in sch2
			oldSqlType := dff.Old.TypeInfo.ToSqlType()
			newSqlType := dff.New.TypeInfo.ToSqlType()

			n0, t0, pk0 := dff.Old.Name, oldSqlType.String(), dff.Old.IsPartOfPK
			n1, t1, pk1 := dff.New.Name, newSqlType.String(), dff.New.IsPartOfPK

			nameLen := 0
			typeLen := 0

			if n0 != n1 {
				n0 = color.YellowString(n0)
				n1 = color.YellowString(n1)
				nameLen = mathutil.Max(len(n0), len(n1))
			}

			if t0 != t1 {
				t0 = color.YellowString(t0)
				t1 = color.YellowString(t1)
				typeLen = mathutil.Max(len(t0), len(t1))
			}

			if pk0 != pk1 {
				if pk0 && !pk1 {
					oldPks = append(oldPks, sql.QuoteIdentifier(n0))
				} else {
					newPks = append(newPks, sql.QuoteIdentifier(n1))
				}
				cli.Println(sql.FmtCol(4, 0, 0, *dff.New))
			} else {
				cli.Println("< " + sql.FmtColWithNameAndType(2, nameLen, typeLen, n0, t0, *dff.Old))
				cli.Println("> " + sql.FmtColWithNameAndType(2, nameLen, typeLen, n1, t1, *dff.New))
			}
		}
	}

	oldPKStr := strings.Join(oldPks, ", ")
	newPKStr := strings.Join(newPks, ", ")

	if oldPKStr != newPKStr {
		cli.Print("< " + color.YellowString(sql.FmtColPrimaryKey(2, oldPKStr)))
		cli.Print("> " + color.YellowString(sql.FmtColPrimaryKey(2, newPKStr)))
	} else {
		cli.Print(sql.FmtColPrimaryKey(4, oldPKStr))
	}

	cli.Println("  );")
	cli.Println()
	return nil
}

func sqlSchemaDiff(tableName string, tags []uint64, diffs map[uint64]diff.SchemaDifference) {
	for _, tag := range tags {
		dff := diffs[tag]
		switch dff.DiffType {
		case diff.SchDiffNone:
		case diff.SchDiffColAdded:
			cli.Println(sql.AlterTableAddColStmt(tableName, sql.FmtCol(0, 0, 0, *dff.New)))
		case diff.SchDiffColRemoved:
			cli.Print(sql.AlterTableDropColStmt(tableName, dff.Old.Name))
		case diff.SchDiffColModified:
			cli.Print(sql.AlterTableRenameColStmt(tableName, dff.Old.Name, dff.New.Name))
		}
	}
}

func dumbDownSchema(in schema.Schema) (schema.Schema, error) {
	allCols := in.GetAllCols()

	dumbCols := make([]schema.Column, 0, allCols.Size())
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.Name = strconv.FormatUint(tag, 10)
		col.Constraints = nil
		dumbCols = append(dumbCols, col)

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	dumbColColl, _ := schema.NewColCollection(dumbCols...)

	return schema.SchemaFromCols(dumbColColl), nil
}

func toNamer(name string) string {
	return diff.To + "_" + name
}

func fromNamer(name string) string {
	return diff.From + "_" + name
}

func diffRows(ctx context.Context, newRows, oldRows types.Map, newSch, oldSch schema.Schema, dArgs *diffArgs, tblName string) errhand.VerboseError {
	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: diff.From, Sch: oldSch},
			{Name: diff.To, Sch: newSch},
		},
		map[string]rowconv.ColNamingFunc{diff.To: toNamer, diff.From: fromNamer},
	)

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	unionSch, ds, verr := createSplitter(newSch, oldSch, joiner, dArgs)
	if verr != nil {
		return verr
	}

	ad := diff.NewAsyncDiffer(1024)
	ad.Start(ctx, newRows, oldRows)
	defer ad.Close()

	src := diff.NewRowDiffSource(ad, joiner)
	defer src.Close()

	oldColNames, verr := mapTagToColName(oldSch, unionSch)

	if verr != nil {
		return verr
	}

	newColNames, verr := mapTagToColName(newSch, unionSch)

	if verr != nil {
		return verr
	}

	schemasEqual := reflect.DeepEqual(oldColNames, newColNames)
	numHeaderRows := 1
	if !schemasEqual {
		numHeaderRows = 2
	}

	var sink DiffSink
	if dArgs.diffOutput == TabularDiffOutput {
		sink, err = diff.NewColorDiffSink(iohelp.NopWrCloser(cli.CliOut), unionSch, numHeaderRows)
	} else {
		sink, err = diff.NewSQLDiffSink(iohelp.NopWrCloser(cli.CliOut), unionSch, tblName)
	}

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	defer sink.Close()

	var badRowVErr errhand.VerboseError
	badRowCallback := func(trf *pipeline.TransformRowFailure) (quit bool) {
		badRowVErr = errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		return true
	}

	p, verr := buildPipeline(dArgs, joiner, ds, unionSch, src, sink, badRowCallback)
	if verr != nil {
		return verr
	}

	if dArgs.diffOutput != SQLDiffOutput {
		if schemasEqual {
			schRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), unionSch, newColNames)

			if err != nil {
				return errhand.BuildDError("error: creating diff header").AddCause(err).Build()
			}

			p.InjectRow(fwtStageName, schRow)
		} else {
			newSchRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), unionSch, oldColNames)

			if err != nil {
				return errhand.BuildDError("error: creating diff header").AddCause(err).Build()
			}

			p.InjectRowWithProps(fwtStageName, newSchRow, map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedOld})
			oldSchRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), unionSch, newColNames)

			if err != nil {
				return errhand.BuildDError("error: creating diff header").AddCause(err).Build()
			}

			p.InjectRowWithProps(fwtStageName, oldSchRow, map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedNew})
		}
	}

	p.Start()
	if err = p.Wait(); err != nil {
		return errhand.BuildDError("Error diffing: %v", err.Error()).Build()
	}

	if badRowVErr != nil {
		return badRowVErr
	}

	return nil
}

func buildPipeline(dArgs *diffArgs, joiner *rowconv.Joiner, ds *diff.DiffSplitter, untypedUnionSch schema.Schema, src *diff.RowDiffSource, sink DiffSink, badRowCB pipeline.BadRowCallback) (*pipeline.Pipeline, errhand.VerboseError) {
	var where FilterFn
	var selTrans *SelectTransform
	where, err := ParseWhere(joiner.GetSchema(), dArgs.where)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to parse where clause").AddCause(err).SetPrintUsage().Build()
	}

	transforms := pipeline.NewTransformCollection()

	if where != nil || dArgs.limit != 0 {
		if where == nil {
			where = func(r row.Row) bool {
				return true
			}
		}

		selTrans = NewSelTrans(where, dArgs.limit)
		transforms.AppendTransforms(pipeline.NewNamedTransform("select", selTrans.LimitAndFilter))
	}

	transforms.AppendTransforms(
		pipeline.NewNamedTransform("split_diffs", ds.SplitDiffIntoOldAndNew),
	)

	if dArgs.diffOutput == TabularDiffOutput {
		nullPrinter := nullprinter.NewNullPrinter(untypedUnionSch)
		fwtTr := fwt.NewAutoSizingFWTTransformer(untypedUnionSch, fwt.HashFillWhenTooLong, 1000)
		transforms.AppendTransforms(
			pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow),
			pipeline.NamedTransform{Name: fwtStageName, Func: fwtTr.TransformToFWT},
		)
	}

	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)
	p := pipeline.NewAsyncPipeline(pipeline.ProcFuncForSourceFunc(src.NextDiff), sinkProcFunc, transforms, badRowCB)
	if selTrans != nil {
		selTrans.Pipeline = p
	}

	return p, nil
}

func mapTagToColName(sch, untypedUnionSch schema.Schema) (map[uint64]string, errhand.VerboseError) {
	tagToCol := make(map[uint64]string)
	allCols := sch.GetAllCols()
	err := untypedUnionSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col, ok := allCols.GetByTag(tag)

		if ok {
			tagToCol[tag] = col.Name
		} else {
			tagToCol[tag] = ""
		}

		return false, nil
	})

	if err != nil {
		return nil, errhand.BuildDError("error: failed to map columns to tags").Build()
	}

	return tagToCol, nil
}

func createSplitter(newSch schema.Schema, oldSch schema.Schema, joiner *rowconv.Joiner, dArgs *diffArgs) (schema.Schema, *diff.DiffSplitter, errhand.VerboseError) {

	var unionSch schema.Schema
	if dArgs.diffOutput == TabularDiffOutput {
		dumbNewSch, err := dumbDownSchema(newSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("").AddCause(err).Build()
		}

		dumbOldSch, err := dumbDownSchema(oldSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("").AddCause(err).Build()
		}

		unionSch, err = untyped.UntypedSchemaUnion(dumbNewSch, dumbOldSch)
		if err != nil {
			return nil, nil, errhand.BuildDError("Failed to merge schemas").Build()
		}

	} else {
		unionSch = newSch
	}

	newToUnionConv := rowconv.IdentityConverter
	if newSch != nil {
		newToUnionMapping, err := rowconv.TagMapping(newSch, unionSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		newToUnionConv, _ = rowconv.NewRowConverter(newToUnionMapping)
	}

	oldToUnionConv := rowconv.IdentityConverter
	if oldSch != nil {
		oldToUnionMapping, err := rowconv.TagMapping(oldSch, unionSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		oldToUnionConv, _ = rowconv.NewRowConverter(oldToUnionMapping)
	}

	ds := diff.NewDiffSplitter(joiner, oldToUnionConv, newToUnionConv)
	return unionSch, ds, nil
}

var emptyHash = hash.Hash{}

func printDocDiffs(ctx context.Context, dEnv *env.DoltEnv, tblName string, tbl1, tbl2 *doltdb.Table, docDetails []doltdb.DocDetails) {
	bold := color.New(color.Bold)

	if docDetails == nil {
		docDetails, _ = dEnv.GetAllValidDocDetails()
	}

	for _, doc := range docDetails {
		if tbl1 != nil {
			sch1, _ := tbl1.GetSchema(ctx)
			doc, _ = doltdb.AddNewerTextToDocFromTbl(ctx, tbl1, &sch1, doc)

		}
		if tbl2 != nil {
			sch2, _ := tbl2.GetSchema(ctx)
			doc, _ = doltdb.AddValueToDocFromTbl(ctx, tbl2, &sch2, doc)
		}

		if doc.Value != nil {
			newer := string(doc.NewerText)
			older, _ := strconv.Unquote(doc.Value.HumanReadableString())
			lines := textdiff.LineDiffAsLines(older, newer)
			if doc.NewerText == nil {
				printDeletedDoc(bold, doc.DocPk, lines)
			} else if len(lines) > 0 && newer != older {
				printModifiedDoc(bold, doc.DocPk, lines)
			}
		} else if doc.Value == nil && doc.NewerText != nil {
			printAddedDoc(bold, doc.DocPk)
		}
	}
}

func printDiffLines(bold *color.Color, lines []string) {
	for _, line := range lines {
		if string(line[0]) == string("+") {
			cli.Println(color.GreenString("+ " + line[1:]))
		} else if string(line[0]) == ("-") {
			cli.Println(color.RedString("- " + line[1:]))
		} else {
			cli.Println(" " + line)
		}
	}
}

func printModifiedDoc(bold *color.Color, pk string, lines []string) {
	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
	_, _ = bold.Printf("--- a/%s\n", pk)
	_, _ = bold.Printf("+++ b/%s\n", pk)

	printDiffLines(bold, lines)
}

func printAddedDoc(bold *color.Color, pk string) {
	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
	_, _ = bold.Println("added doc")
}

func printDeletedDoc(bold *color.Color, pk string, lines []string) {
	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
	_, _ = bold.Println("deleted doc")

	printDiffLines(bold, lines)
}

func printTableDiffSummary(ctx context.Context, dEnv *env.DoltEnv, tblName string, tbl1, tbl2 *doltdb.Table, docDetails []doltdb.DocDetails) {
	bold := color.New(color.Bold)

	if tblName == doltdb.DocTableName {
		printDocDiffs(ctx, dEnv, tblName, tbl1, tbl2, docDetails)
	} else {
		_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", tblName)

		if tbl1 == nil {
			_, _ = bold.Println("deleted table")
		} else if tbl2 == nil {
			_, _ = bold.Println("added table")
		} else {
			h1, err := tbl1.HashOf()

			if err != nil {
				panic(err)
			}

			_, _ = bold.Printf("--- a/%s @ %s\n", tblName, h1.String())

			h2, err := tbl2.HashOf()

			if err != nil {
				panic(err)
			}

			_, _ = bold.Printf("+++ b/%s @ %s\n", tblName, h2.String())
		}
	}
}

func diffSummary(ctx context.Context, v1, v2 types.Map, colLen int) errhand.VerboseError {
	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.Summary(ctx, ch, v1, v2)

		ae.SetIfError(err)
	}()

	acc := diff.DiffSummaryProgress{}
	var count int64
	var pos int
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.CellChanges += p.CellChanges
		acc.NewSize += p.NewSize
		acc.OldSize += p.OldSize

		if count%10000 == 0 {
			statusStr := fmt.Sprintf("prev size: %d, new size: %d, adds: %d, deletes: %d, modifications: %d", acc.OldSize, acc.NewSize, acc.Adds, acc.Removes, acc.Changes)
			pos = cli.DeleteAndPrint(pos, statusStr)
		}

		count++
	}

	pos = cli.DeleteAndPrint(pos, "")

	if err := ae.Get(); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	if acc.NewSize > 0 || acc.OldSize > 0 {
		formatSummary(acc, colLen)
	} else {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
	}

	return nil
}

func formatSummary(acc diff.DiffSummaryProgress, colLen int) {
	pluralize := func(singular, plural string, n uint64) string {
		var noun string
		if n != 1 {
			noun = plural
		} else {
			noun = singular
		}
		return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
	}

	rowsUnmodified := uint64(acc.OldSize - acc.Changes - acc.Removes)
	unmodified := pluralize("Row Unmodified", "Rows Unmodified", rowsUnmodified)
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)
	changes := pluralize("Row Modified", "Rows Modified", acc.Changes)
	cellChanges := pluralize("Cell Modified", "Cells Modified", acc.CellChanges)

	oldValues := pluralize("Entry", "Entries", acc.OldSize)
	newValues := pluralize("Entry", "Entries", acc.NewSize)

	percentCellsChanged := float64(100*acc.CellChanges) / (float64(acc.OldSize) * float64(colLen))

	cli.Printf("%s (%.2f%%)\n", unmodified, (float64(100*rowsUnmodified) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", insertions, (float64(100*acc.Adds) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", deletions, (float64(100*acc.Removes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", changes, (float64(100*acc.Changes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", cellChanges, percentCellsChanged)
	cli.Printf("(%s vs %s)\n\n", oldValues, newValues)
}
