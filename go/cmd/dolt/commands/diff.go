// Copyright 2019 Dolthub, Inc.
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
	"reflect"
	"sort"
	"strconv"
	"strings"

	textdiff "github.com/andreyvit/diff"
	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/types"
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
	CachedFlag  = "cached"
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
	tableSet   *set.StrSet
	docSet     *set.StrSet
	limit      int
	where      string
	query      string
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
func (cmd DiffCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, diffDocs, ap))
}

func (cmd DiffCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(DataFlag, "d", "Show only the data changes, do not show the schema changes (Both shown by default).")
	ap.SupportsFlag(SchemaFlag, "s", "Show only the schema changes, do not show the data changes (Both shown by default).")
	ap.SupportsFlag(SummaryFlag, "", "Show summary of data changes")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format diff output. Valid values are tabular & sql. Defaults to tabular. ")
	ap.SupportsString(whereParam, "", "column", "filters columns based on values in the diff.  See {{.EmphasisLeft}}dolt diff --help{{.EmphasisRight}} for details.")
	ap.SupportsInt(limitParam, "", "record_count", "limits to the first N diffs.")
	ap.SupportsFlag(CachedFlag, "c", "Show only the unstaged data changes.")
	return ap
}

// Exec executes the command
func (cmd DiffCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, diffDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	fromRoot, toRoot, dArgs, err := parseDiffArgs(ctx, dEnv, apr)

	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	verr := diffUserTables(ctx, fromRoot, toRoot, dArgs)

	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	err = diffDoltDocs(ctx, dEnv, fromRoot, toRoot, dArgs)

	if err != nil {
		verr = errhand.BuildDError("error diffing dolt docs").AddCause(err).Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func parseDiffArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (from, to *doltdb.RootValue, dArgs *diffArgs, err error) {
	dArgs = &diffArgs{}

	if q, ok := apr.GetValue(QueryFlag); ok {
		_, okWhere := apr.GetValue(whereParam)
		_, okLimit := apr.GetInt(limitParam)
		switch {
		case okWhere:
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, whereParam)
		case okLimit:
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, limitParam)
		case apr.Contains(DataFlag):
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, DataFlag)
		case apr.Contains(SchemaFlag):
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, SchemaFlag)
		case apr.Contains(SummaryFlag):
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, SummaryFlag)
		case apr.Contains(SQLFlag):
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, SQLFlag)
		case apr.Contains(CachedFlag):
			return nil, nil, nil, fmt.Errorf("arg %s cannot be combined with arg %s", QueryFlag, CachedFlag)
		}
		dArgs.query = q
	}

	dArgs.diffParts = SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		dArgs.diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		dArgs.diffParts = SchemaOnlyDiff
	}

	f, _ := apr.GetValue(FormatFlag)
	switch strings.ToLower(f) {
	case "tabular":
		dArgs.diffOutput = TabularDiffOutput
	case "sql":
		dArgs.diffOutput = SQLDiffOutput
	case "":
		dArgs.diffOutput = TabularDiffOutput
	default:
		return nil, nil, nil, fmt.Errorf("invalid output format: %s", f)
	}

	if apr.Contains(SummaryFlag) {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) {
			return nil, nil, nil, fmt.Errorf("invalid Arguments: --summary cannot be combined with --schema or --data")
		}
		dArgs.diffParts = Summary
	}

	dArgs.limit, _ = apr.GetInt(limitParam)
	dArgs.where = apr.GetValueOrDefault(whereParam, "")

	from, to, leftover, err := getDiffRoots(ctx, dEnv, apr.Args, apr.Contains(CachedFlag))

	if err != nil {
		return nil, nil, nil, err
	}

	if dArgs.query != "" && len(leftover) != 0 {
		return nil, nil, nil, fmt.Errorf("too many arguments, diff -q does not take table args")
	}

	dArgs.tableSet = set.NewStrSet(nil)
	dArgs.docSet = set.NewStrSet(nil)

	for _, arg := range leftover {
		if arg == doltdocs.ReadmeDoc || arg == doltdocs.LicenseDoc {
			dArgs.docSet.Add(arg)
			continue
		}

		// verify table args exist in at least one root
		_, ok, err := from.GetTable(ctx, arg)
		if err != nil {
			return nil, nil, nil, err
		}
		if ok {
			dArgs.tableSet.Add(arg)
			continue
		}

		_, ok, err = to.GetTable(ctx, arg)
		if err != nil {
			return nil, nil, nil, err
		}
		if !ok {
			return nil, nil, nil, fmt.Errorf("table %s does not exist in either diff root", arg)
		}
	}

	// if no tables or docs were specified as args, diff all tables and docs
	if len(leftover) == 0 {
		utn, err := doltdb.UnionTableNames(ctx, from, to)
		if err != nil {
			return nil, nil, nil, err
		}
		dArgs.tableSet.Add(utn...)
		dArgs.docSet.Add(doltdocs.ReadmeDoc, doltdocs.LicenseDoc)
	}

	return from, to, dArgs, nil
}

func getDiffRoots(ctx context.Context, dEnv *env.DoltEnv, args []string, isCached bool) (from, to *doltdb.RootValue, leftover []string, err error) {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	docs, err := dEnv.DocsReadWriter().GetDocsOnDisk()
	if err != nil {
		return nil, nil, nil, err
	}

	workingRoot, err = doltdocs.UpdateRootWithDocs(ctx, workingRoot, docs)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(args) == 0 {
		// `dolt diff`
		from = stagedRoot
		to = workingRoot
		if isCached {
			from = headRoot
			to = stagedRoot
		}
		return from, to, nil, nil
	}

	from, ok := maybeResolve(ctx, dEnv, args[0])

	if !ok {
		// `dolt diff ...tables`
		from = stagedRoot
		to = workingRoot
		if isCached {
			from = headRoot
			to = stagedRoot
		}
		leftover = args
		return from, to, leftover, nil
	}

	if len(args) == 1 {
		// `dolt diff from_commit`
		to = workingRoot
		if isCached {
			to = stagedRoot
		}
		return from, to, nil, nil
	}

	to, ok = maybeResolve(ctx, dEnv, args[1])

	if !ok {
		// `dolt diff from_commit ...tables`
		to = workingRoot
		if isCached {
			to = stagedRoot
		}
		leftover = args[1:]
		return from, to, leftover, nil
	}

	// `dolt diff from_commit to_commit ...tables`
	leftover = args[2:]
	return from, to, leftover, nil
}

// todo: distinguish between non-existent CommitSpec and other errors, don't assume non-existent
func maybeResolve(ctx context.Context, dEnv *env.DoltEnv, spec string) (*doltdb.RootValue, bool) {
	cs, err := doltdb.NewCommitSpec(spec)
	if err != nil {
		return nil, false
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return nil, false
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, false
	}

	return root, true
}

func diffUserTables(ctx context.Context, fromRoot, toRoot *doltdb.RootValue, dArgs *diffArgs) (verr errhand.VerboseError) {
	var err error

	tableDeltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
	}

	sort.Slice(tableDeltas, func(i, j int) bool {
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})
	for _, td := range tableDeltas {
		if !dArgs.tableSet.Contains(td.FromName) && !dArgs.tableSet.Contains(td.ToName) {
			continue
		}

		tblName := td.ToName
		fromTable := td.FromTable
		toTable := td.ToTable

		if fromTable == nil && toTable == nil {
			return errhand.BuildDError("error: both tables in tableDelta are nil").Build()
		}

		if dArgs.diffOutput == TabularDiffOutput {
			printTableDiffSummary(td)

			// if we're in standard output mode, follow Git convention
			// and don't print data diffs for added/dropped tables
			if td.IsDrop() || td.IsAdd() {
				continue
			}
		}

		if tblName == doltdb.DocTableName {
			continue
		}

		fromSch, toSch, err := td.GetSchemas(ctx)
		if err != nil {
			return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
		}

		if dArgs.diffParts&Summary != 0 {
			numCols := fromSch.GetAllCols().Size()
			verr = diffSummary(ctx, td, numCols)
		}

		if dArgs.diffParts&SchemaOnlyDiff != 0 {
			verr = diffSchemas(ctx, toRoot, td, dArgs)
		}

		if dArgs.diffParts&DataOnlyDiff != 0 {
			if td.IsDrop() && dArgs.diffOutput == SQLDiffOutput {
				continue // don't output DELETE FROM statements after DROP TABLE
			} else if td.IsAdd() {
				fromSch = toSch
			}
			verr = diffRows(ctx, td, dArgs, toRoot.VRW())
		}

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffSchemas(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta, dArgs *diffArgs) errhand.VerboseError {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("could not read schemas from toRoot").AddCause(err).Build()
	}

	if dArgs.diffOutput == TabularDiffOutput {
		if td.IsDrop() || td.IsAdd() {
			panic("cannot perform tabular schema diff for added/dropped tables")
		}

		return printShowCreateTableDiff(ctx, td)
	}

	return sqlSchemaDiff(ctx, td, toSchemas)
}

func printShowCreateTableDiff(ctx context.Context, td diff.TableDelta) errhand.VerboseError {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	sqlDb := sqle.NewSingleTableDatabase(td.ToName, fromSch, td.FromFks, td.FromFksParentSch)
	sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
	fromCreateStmt, err := sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)

	sqlDb = sqle.NewSingleTableDatabase(td.ToName, toSch, td.ToFks, td.ToFksParentSch)
	sqlCtx, engine, _ = sqle.PrepareCreateTableStmt(ctx, sqlDb)
	toCreateStmt, err := sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)

	if fromCreateStmt != toCreateStmt {
		cli.Println(textdiff.LineDiff(fromCreateStmt, toCreateStmt))
	}

	return nil
}

// TODO: this doesn't handle check constraints or triggers
func sqlSchemaDiff(ctx context.Context, td diff.TableDelta, toSchemas map[string]schema.Schema) errhand.VerboseError {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	if td.IsDrop() {
		cli.Println(sqlfmt.DropTableStmt(td.FromName))
	} else if td.IsAdd() {
		sqlDb := sqle.NewSingleTableDatabase(td.ToName, toSch, td.ToFks, td.ToFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		stmt, err := sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		cli.Println(stmt)
	} else {
		if td.FromName != td.ToName {
			cli.Println(sqlfmt.RenameTableStmt(td.FromName, td.ToName))
		}

		eq := schema.SchemasAreEqual(fromSch, toSch)
		if eq && !td.HasFKChanges() {
			return nil
		}

		colDiffs, unionTags := diff.DiffSchColumns(fromSch, toSch)
		for _, tag := range unionTags {
			cd := colDiffs[tag]
			switch cd.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				cli.Println(sqlfmt.AlterTableAddColStmt(td.ToName, sqlfmt.FmtCol(0, 0, 0, *cd.New)))
			case diff.SchDiffRemoved:
				cli.Println(sqlfmt.AlterTableDropColStmt(td.ToName, cd.Old.Name))
			case diff.SchDiffModified:
				// Ignore any primary key set changes here
				if cd.Old.IsPartOfPK != cd.New.IsPartOfPK {
					continue
				}
				if cd.Old.Name != cd.New.Name {
					cli.Println(sqlfmt.AlterTableRenameColStmt(td.ToName, cd.Old.Name, cd.New.Name))
				}
			}
		}

		// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
		if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
			cli.Println(sqlfmt.AlterTableDropPks(td.ToName))
			if toSch.GetPKCols().Size() > 0 {
				cli.Println(sqlfmt.AlterTableAddPrimaryKeys(td.ToName, toSch.GetPKCols()))
			}
		}

		for _, idxDiff := range diff.DiffSchIndexes(fromSch, toSch) {
			switch idxDiff.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				cli.Println(sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
			case diff.SchDiffRemoved:
				cli.Println(sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
			case diff.SchDiffModified:
				cli.Println(sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
				cli.Println(sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
			}
		}

		for _, fkDiff := range diff.DiffForeignKeys(td.FromFks, td.ToFks) {
			switch fkDiff.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				parentSch := toSchemas[fkDiff.To.ReferencedTableName]
				cli.Println(sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
			case diff.SchDiffRemoved:
				cli.Println(sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
			case diff.SchDiffModified:
				cli.Println(sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
				parentSch := toSchemas[fkDiff.To.ReferencedTableName]
				cli.Println(sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
			}
		}
	}
	return nil
}

func dumbDownSchema(in schema.Schema) (schema.Schema, error) {
	allCols := in.GetAllCols()

	dumbCols := make([]schema.Column, 0, allCols.Size())
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.Name = strconv.FormatUint(tag, 10)
		col.Constraints = nil
		col.Default = ""
		dumbCols = append(dumbCols, col)

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	dumbColColl := schema.NewColCollection(dumbCols...)

	return schema.SchemaFromCols(dumbColColl)
}

func toNamer(name string) string {
	return diff.To + "_" + name
}

func fromNamer(name string) string {
	return diff.From + "_" + name
}

func diffRows(ctx context.Context, td diff.TableDelta, dArgs *diffArgs, vrw types.ValueReadWriter) errhand.VerboseError {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}
	if td.IsAdd() {
		fromSch = toSch
	}

	fromRows, toRows, err := td.GetMaps(ctx)
	if err != nil {
		return errhand.BuildDError("could not get row data for table %s", td.ToName).AddCause(err).Build()
	}

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: diff.From, Sch: fromSch},
			{Name: diff.To, Sch: toSch},
		},
		map[string]rowconv.ColNamingFunc{diff.To: toNamer, diff.From: fromNamer},
	)

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	unionSch, ds, verr := createSplitter(ctx, vrw, fromSch, toSch, joiner, dArgs)
	if verr != nil {
		return verr
	}

	rd := diff.NewRowDiffer(ctx, fromSch, toSch, 1024)
	if _, ok := rd.(*diff.EmptyRowDiffer); ok {
		cli.Println("warning: skipping data diff due to primary key set change")
		return nil
	}
	rd.Start(ctx, fromRows, toRows)
	defer rd.Close()

	src := diff.NewRowDiffSource(rd, joiner, nil)
	defer src.Close()

	oldColNames, verr := mapTagToColName(fromSch, unionSch)

	if verr != nil {
		return verr
	}

	newColNames, verr := mapTagToColName(toSch, unionSch)

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
		sink, err = diff.NewSQLDiffSink(iohelp.NopWrCloser(cli.CliOut), unionSch, td.CurName())
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
			schRow, err := untyped.NewRowFromTaggedStrings(toRows.Format(), unionSch, newColNames)

			if err != nil {
				return errhand.BuildDError("error: creating diff header").AddCause(err).Build()
			}

			p.InjectRow(fwtStageName, schRow)
		} else {
			newSchRow, err := untyped.NewRowFromTaggedStrings(toRows.Format(), unionSch, oldColNames)

			if err != nil {
				return errhand.BuildDError("error: creating diff header").AddCause(err).Build()
			}

			p.InjectRowWithProps(fwtStageName, newSchRow, map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedOld})
			oldSchRow, err := untyped.NewRowFromTaggedStrings(toRows.Format(), unionSch, newColNames)

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

func createSplitter(ctx context.Context, vrw types.ValueReadWriter, fromSch schema.Schema, toSch schema.Schema, joiner *rowconv.Joiner, dArgs *diffArgs) (schema.Schema, *diff.DiffSplitter, errhand.VerboseError) {

	var unionSch schema.Schema
	if dArgs.diffOutput == TabularDiffOutput {
		dumbNewSch, err := dumbDownSchema(toSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("").AddCause(err).Build()
		}

		dumbOldSch, err := dumbDownSchema(fromSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("").AddCause(err).Build()
		}

		unionSch, err = untyped.UntypedSchemaUnion(dumbNewSch, dumbOldSch)
		if err != nil {
			return nil, nil, errhand.BuildDError("Merge failed. Tables with different primary keys cannot be merged.").AddCause(err).Build()
		}

	} else {
		unionSch = toSch
	}

	newToUnionConv := rowconv.IdentityConverter
	if toSch != nil {
		newToUnionMapping, err := rowconv.TagMapping(toSch, unionSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		newToUnionConv, _ = rowconv.NewRowConverter(ctx, vrw, newToUnionMapping)
	}

	oldToUnionConv := rowconv.IdentityConverter
	if fromSch != nil {
		oldToUnionMapping, err := rowconv.TagMapping(fromSch, unionSch)

		if err != nil {
			return nil, nil, errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		oldToUnionConv, _ = rowconv.NewRowConverter(ctx, vrw, oldToUnionMapping)
	}

	ds := diff.NewDiffSplitter(joiner, oldToUnionConv, newToUnionConv)
	return unionSch, ds, nil
}

func diffDoltDocs(ctx context.Context, dEnv *env.DoltEnv, from, to *doltdb.RootValue, dArgs *diffArgs) error {
	_, docs, err := actions.GetTablesOrDocs(dEnv.DocsReadWriter(), dArgs.docSet.AsSlice())

	if err != nil {
		return err
	}

	return printDocDiffs(ctx, from, to, docs)
}

func printDocDiffs(ctx context.Context, from, to *doltdb.RootValue, docsFilter doltdocs.Docs) error {
	bold := color.New(color.Bold)

	comparisons, err := diff.DocsDiffToComparisons(ctx, from, to, docsFilter)
	if err != nil {
		return err
	}

	for _, doc := range docsFilter {
		for _, comparison := range comparisons {
			if doc.DocPk == comparison.DocName {
				if comparison.OldText == nil && comparison.CurrentText != nil {
					printAddedDoc(bold, comparison.DocName)
				} else if comparison.OldText != nil {
					older := string(comparison.OldText)
					newer := string(comparison.CurrentText)

					lines := textdiff.LineDiffAsLines(older, newer)

					if comparison.CurrentText == nil {
						printDeletedDoc(bold, comparison.DocName, lines)
					} else if len(lines) > 0 && newer != older {
						printModifiedDoc(bold, comparison.DocName, lines)
					}
				}
			}
		}
	}

	return nil
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

func printTableDiffSummary(td diff.TableDelta) {
	bold := color.New(color.Bold)
	if td.IsDrop() {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.FromName, td.FromName)
		_, _ = bold.Println("deleted table")
	} else if td.IsAdd() {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.ToName, td.ToName)
		_, _ = bold.Println("added table")
	} else {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.FromName, td.ToName)
		h1, err := td.FromTable.HashOf()

		if err != nil {
			panic(err)
		}

		_, _ = bold.Printf("--- a/%s @ %s\n", td.FromName, h1.String())

		h2, err := td.ToTable.HashOf()

		if err != nil {
			panic(err)
		}

		_, _ = bold.Printf("+++ b/%s @ %s\n", td.ToName, h2.String())
	}
}

func diffSummary(ctx context.Context, td diff.TableDelta, colLen int) errhand.VerboseError {
	// todo: use errgroup.Group
	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.SummaryForTableDelta(ctx, ch, td)

		ae.SetIfError(err)
	}()

	acc := diff.DiffSummaryProgress{}
	var count int64
	var pos int
	eP := cli.NewEphemeralPrinter()
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
			eP.Printf("prev size: %d, new size: %d, adds: %d, deletes: %d, modifications: %d\n", acc.OldSize, acc.NewSize, acc.Adds, acc.Removes, acc.Changes)
			eP.Display()
		}

		count++
	}

	pos = cli.DeleteAndPrint(pos, "")

	if err := ae.Get(); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return nil
	}

	if (acc.Adds + acc.Removes + acc.Changes) == 0 {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
		return nil
	}

	if keyless {
		printKeylessSummary(acc)
	} else {
		printSummary(acc, colLen)
	}

	return nil
}

func printSummary(acc diff.DiffSummaryProgress, colLen int) {
	rowsUnmodified := uint64(acc.OldSize - acc.Changes - acc.Removes)
	unmodified := pluralize("Row Unmodified", "Rows Unmodified", rowsUnmodified)
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)
	changes := pluralize("Row Modified", "Rows Modified", acc.Changes)
	cellChanges := pluralize("Cell Modified", "Cells Modified", acc.CellChanges)

	oldValues := pluralize("Entry", "Entries", acc.OldSize)
	newValues := pluralize("Entry", "Entries", acc.NewSize)

	percentCellsChanged := float64(100*acc.CellChanges) / (float64(acc.OldSize) * float64(colLen))

	safePercent := func(num, dom uint64) float64 {
		// returns +Inf for x/0 where x > 0
		if num == 0 {
			return float64(0)
		}
		return float64(100*num) / (float64(dom))
	}

	cli.Printf("%s (%.2f%%)\n", unmodified, safePercent(rowsUnmodified, acc.OldSize))
	cli.Printf("%s (%.2f%%)\n", insertions, safePercent(acc.Adds, acc.OldSize))
	cli.Printf("%s (%.2f%%)\n", deletions, safePercent(acc.Removes, acc.OldSize))
	cli.Printf("%s (%.2f%%)\n", changes, safePercent(acc.Changes, acc.OldSize))
	cli.Printf("%s (%.2f%%)\n", cellChanges, percentCellsChanged)
	cli.Printf("(%s vs %s)\n\n", oldValues, newValues)
}

func printKeylessSummary(acc diff.DiffSummaryProgress) {
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)

	cli.Printf("%s\n", insertions)
	cli.Printf("%s\n", deletions)
}

func pluralize(singular, plural string, n uint64) string {
	var noun string
	if n != 1 {
		noun = plural
	} else {
		noun = singular
	}
	return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
}
