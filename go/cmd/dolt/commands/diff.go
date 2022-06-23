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
	"sort"
	"strings"

	textdiff "github.com/andreyvit/diff"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/go-mysql-server/sql"
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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/atomicerr"
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

To filter which data rows are displayed, use {{.EmphasisLeft}}--where <SQL expression>{{.EmphasisRight}}. Table column names in the filter expression must be prefixed with {{.EmphasisLeft}}from_{{.EmphasisRight}} or {{.EmphasisLeft}}to_{{.EmphasisRight}}, e.g. {{.EmphasisLeft}}to_COLUMN_NAME > 100{{.EmphasisRight}} or {{.EmphasisLeft}}from_COLUMN_NAME + to_COLUMN_NAME = 0{{.EmphasisRight}}.
`,
	Synopsis: []string{
		`[options] [{{.LessThan}}commit{{.GreaterThan}}] [{{.LessThan}}tables{{.GreaterThan}}...]`,
		`[options] {{.LessThan}}commit{{.GreaterThan}} {{.LessThan}}commit{{.GreaterThan}} [{{.LessThan}}tables{{.GreaterThan}}...]`,
	},
}

type diffArgs struct {
	diffParts  diffPart
	diffOutput diffOutput
	fromRoot   *doltdb.RootValue
	toRoot  *doltdb.RootValue
	fromRef string
	toRef   string
	tableSet *set.StrSet
	docSet     *set.StrSet
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

func (cmd DiffCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(diffDocs, ap)
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
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, diffDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	verr := cmd.validateArgs(apr)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	dArgs, err := parseDiffArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	verr = diffUserTables(ctx, dEnv, dArgs)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	err = diffDoltDocs(ctx, dEnv, dArgs)
	if err != nil {
		verr = errhand.BuildDError("error diffing dolt docs").AddCause(err).Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func (cmd DiffCmd) validateArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.Contains(SummaryFlag) {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) {
			return errhand.BuildDError("invalid Arguments: --summary cannot be combined with --schema or --data").Build()
		}
	}

	f, _ := apr.GetValue(FormatFlag)
	switch strings.ToLower(f) {
	case "tabular", "sql", "":
	default:
		return errhand.BuildDError("invalid output format: %s", f).Build()
	}


	return nil
}

func parseDiffArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*diffArgs, error) {
	dArgs := &diffArgs{}

	dArgs.diffParts = SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		dArgs.diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		dArgs.diffParts = SchemaOnlyDiff
	} else if apr.Contains(SummaryFlag) {
		dArgs.diffParts = Summary
	}

	f := apr.GetValueOrDefault(FormatFlag, "tabular")
	switch strings.ToLower(f) {
	case "tabular":
		dArgs.diffOutput = TabularDiffOutput
	case "sql":
		dArgs.diffOutput = SQLDiffOutput
	}

	dArgs.limit, _ = apr.GetInt(limitParam)
	dArgs.where = apr.GetValueOrDefault(whereParam, "")

	tableNames, err := dArgs.applyDiffRoots(ctx, dEnv, apr.Args, apr.Contains(CachedFlag))
	if err != nil {
		return nil, err
	}

	dArgs.tableSet = set.NewStrSet(nil)
	dArgs.docSet = set.NewStrSet(nil)

	for _, tableName := range tableNames {
		if tableName == doltdocs.ReadmeDoc || tableName == doltdocs.LicenseDoc {
			dArgs.docSet.Add(tableName)
			continue
		}

		// verify table args exist in at least one root
		_, ok, err := dArgs.fromRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if ok {
			dArgs.tableSet.Add(tableName)
			continue
		}

		_, ok, err = dArgs.toRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("table %s does not exist in either revision", tableName)
		}
	}

	// if no tables or docs were specified as args, diff all tables and docs
	if len(tableNames) == 0 {
		utn, err := doltdb.UnionTableNames(ctx, dArgs.fromRoot, dArgs.toRoot)
		if err != nil {
			return nil, err
		}
		dArgs.tableSet.Add(utn...)
		dArgs.docSet.Add(doltdocs.ReadmeDoc, doltdocs.LicenseDoc)
	}

	return dArgs, nil
}

// applyDiffRoots applies the appropriate |from| and |to| root values to the receiver and returns the table names
// (if any) given to the command.
func (dArgs *diffArgs) applyDiffRoots(ctx context.Context, dEnv *env.DoltEnv, args []string, isCached bool) ([]string, error) {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return nil, err
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, err
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	docs, err := dEnv.DocsReadWriter().GetDocsOnDisk()
	if err != nil {
		return nil, err
	}

	workingRoot, err = doltdocs.UpdateRootWithDocs(ctx, workingRoot, docs)
	if err != nil {
		return nil, err
	}

	dArgs.fromRoot = stagedRoot
	dArgs.fromRef = "STAGED"
	dArgs.toRoot = workingRoot
	dArgs.toRef = "WORKING"
	if isCached {
		dArgs.fromRoot = headRoot
		dArgs.fromRef = "HEAD"
		dArgs.toRoot = stagedRoot
		dArgs.toRef = "STAGED"
	}

	if len(args) == 0 {
		// `dolt diff`
		return nil, nil
	}

	// treat the first arg as a ref spec
	fromRoot, ok := maybeResolve(ctx, dEnv, args[0])

	// if it doesn't resolve, treat it as a table name
	if !ok {
		return args, nil
	}

	dArgs.fromRoot = fromRoot
	dArgs.fromRef = args[0]

	if len(args) == 1 {
		// `dolt diff from_commit`
		return nil, nil
	}

	toRoot, ok := maybeResolve(ctx, dEnv, args[1])

	if !ok {
		// `dolt diff from_commit ...tables`
		return args[1:], nil
	}

	dArgs.toRoot = toRoot
	dArgs.toRef = args[1]

	// `dolt diff from_commit to_commit ...tables`
	return args[2:], nil
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

func diffUserTables(ctx context.Context, dEnv *env.DoltEnv, dArgs *diffArgs) (verr errhand.VerboseError) {
	var err error

	tableDeltas, err := diff.GetTableDeltas(ctx, dArgs.fromRoot, dArgs.toRoot)
	if err != nil {
		return errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
	}

	engine, err := newSqlEngine(ctx, dEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
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
			verr = printDiffSummary(ctx, td, numCols)
		}

		if dArgs.diffParts&SchemaOnlyDiff != 0 {
			verr = diffSchemas(ctx, dArgs.toRoot, td, dArgs)
		}

		if dArgs.diffParts&DataOnlyDiff != 0 {
			if td.IsDrop() && dArgs.diffOutput == SQLDiffOutput {
				continue // don't output DELETE FROM statements after DROP TABLE
			} else if td.IsAdd() {
				fromSch = toSch
			}

			if !schema.ArePrimaryKeySetsDiffable(fromSch, toSch) {
				cli.PrintErrf("Primary key sets differ between revisions for table %s, skipping data diff\n", tblName)
				continue
			}

			verr = diffRows(ctx, engine, td, dArgs)
		}

		if verr != nil {
			return verr
		}
	}

	return nil
}

func newSqlEngine(ctx context.Context, dEnv *env.DoltEnv) (*engine.SqlEngine, error) {
	mrEnv, err := env.DoltEnvAsMultiEnv(ctx, dEnv)
	if err != nil {
		return nil, err
	}

	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var dbName string
	mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		dbName = name
		return true, nil
	})

	return engine.NewSqlEngine(
		ctx,
		mrEnv,
		engine.FormatCsv,
		&engine.SqlEngineConfig{
			InitialDb:    dbName,
			IsReadOnly:   false,
			ServerUser:   "root",
			Autocommit:   false,
		},
	)
}

// TODO: engine should do this for me
func newSqlContext(ctx context.Context, se *engine.SqlEngine) (*sql.Context, error) {
	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	sqlCtx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})
	return sqlCtx, nil
}


func diffSchemas(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta, dArgs *diffArgs) errhand.VerboseError {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("could not read schemas from toRoot").AddCause(err).Build()
	}

	if dArgs.diffOutput == TabularDiffOutput {
		return printShowCreateTableDiff(ctx, td)
	}

	return sqlSchemaDiff(ctx, td, toSchemas)
}

func printShowCreateTableDiff(ctx context.Context, td diff.TableDelta) errhand.VerboseError {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	var fromCreateStmt = ""
	if td.FromTable != nil {
		sqlDb := sqle.NewSingleTableDatabase(td.FromName, fromSch, td.FromFks, td.FromFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		fromCreateStmt, err = sqle.GetCreateTableStmt(sqlCtx, engine, td.FromName)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	var toCreateStmt = ""
	if td.ToTable != nil {
		sqlDb := sqle.NewSingleTableDatabase(td.ToName, toSch, td.ToFks, td.ToFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		toCreateStmt, err = sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

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

func diffRows(ctx context.Context, engine *engine.SqlEngine, td diff.TableDelta, dArgs *diffArgs) errhand.VerboseError {
	from, to := dArgs.fromRef, dArgs.toRef

	tableName := td.ToName
	if len(tableName) == 0 {
		tableName = td.FromName
	}

	columns := getColumnNamesString(td.FromSch, td.ToSch)
	query := fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columns, "diff_type", tableName, from, to)

	if len(dArgs.where) > 0 {
		query += " where " + dArgs.where
	}

	sqlCtx, err := newSqlContext(ctx, engine)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sch, rowIter, err := engine.Query(sqlCtx, query)
	if sql.ErrSyntaxError.Is(err) {
		return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", query).AddCause(err).Build()
	} else if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
	}

	defer rowIter.Close(sqlCtx)

	var toSch, fromSch sql.Schema
	if td.FromSch != nil {
		pkSch, err := sqlutil.FromDoltSchema(td.FromName, td.FromSch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		fromSch = pkSch.Schema
	}

	if td.ToSch != nil {
		pkSch, err := sqlutil.FromDoltSchema(td.ToName, td.ToSch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		toSch = pkSch.Schema
	}

	unionSch := unionSchemas(fromSch, toSch)

	// TODO: default sample size
	resultsWriter := tabular.NewFixedWidthDiffTableWriter(unionSch, iohelp.NopWrCloser(cli.CliOut), 100)

		// TODO: SQL writer
		// sink, err = diff.NewSQLDiffSink(iohelp.NopWrCloser(cli.CliOut), td.ToSch, td.CurName())

	err = writeDiffResults(sqlCtx, sch, unionSch, rowIter, resultsWriter)
	if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
	}

	return nil
}

func unionSchemas(s1 sql.Schema, s2 sql.Schema) sql.Schema {
	var union sql.Schema
	for i := range s1 {
		union = append(union, s1[i])
	}
	for i := range s2 {
		if union.IndexOfColName(s2[i].Name) < 0 {
			union = append(union, s2[i])
		}
	}
	return union
}

// TODO: SQL writer
func writeDiffResults(ctx *sql.Context, diffQuerySch sql.Schema, targetSch sql.Schema, iter sql.RowIter, writer *tabular.FixedWidthDiffTableWriter, ) error {
	ds, err := newDiffSplitter(diffQuerySch, targetSch)
	if err != nil {
		return err
	}

	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			return writer.Close(ctx)
		} else if err != nil {
			return err
		}

		oldRow, newRow, err := ds.splitDiffResultRow(r)
		if err != nil {
			return err
		}

		if oldRow.row != nil {
			err := writer.WriteRow(ctx, oldRow.row, oldRow.rowDiff, oldRow.colDiffs)
			if err != nil {
				return err
			}
		}

		if newRow.row != nil {
			err := writer.WriteRow(ctx, newRow.row, newRow.rowDiff, newRow.colDiffs)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getColumnNamesString(fromSch, toSch schema.Schema) string {
	var cols []string
	if fromSch != nil {
		fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("cast (from_%s as char) as `from_%s`", col.Name, col.Name))
			return false, nil
		})
	}
	if toSch != nil {
		toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("cast (to_%s as char) as `to_%s`", col.Name, col.Name))
			return false, nil
		})
	}
	return strings.Join(cols, ",")
}

func diffDoltDocs(ctx context.Context, dEnv *env.DoltEnv, dArgs *diffArgs) error {
	_, docs, err := actions.GetTablesOrDocs(dEnv.DocsReadWriter(), dArgs.docSet.AsSlice())

	if err != nil {
		return err
	}

	return printDocDiffs(ctx, dArgs.fromRoot, dArgs.toRoot, docs)
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

func printDiffSummary(ctx context.Context, td diff.TableDelta, colLen int) errhand.VerboseError {
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
