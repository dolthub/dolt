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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
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
	JsonDiffOutput    diffOutput = 3

	DataFlag    = "data"
	SchemaFlag  = "schema"
	SummaryFlag = "summary"
	whereParam  = "where"
	limitParam  = "limit"
	SQLFlag     = "sql"
	CachedFlag  = "cached"
	SkinnyFlag  = "skinny"
)

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
	toRoot     *doltdb.RootValue
	fromRef    string
	toRef      string
	tableSet   *set.StrSet
	limit      int
	where      string
	skinny     bool
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
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format diff output. Valid values are tabular, sql, json. Defaults to tabular.")
	ap.SupportsString(whereParam, "", "column", "filters columns based on values in the diff.  See {{.EmphasisLeft}}dolt diff --help{{.EmphasisRight}} for details.")
	ap.SupportsInt(limitParam, "", "record_count", "limits to the first N diffs.")
	ap.SupportsFlag(CachedFlag, "c", "Show only the unstaged data changes.")
	ap.SupportsFlag(SkinnyFlag, "sk", "Shows only primary key columns and any columns with data changes.")
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
	case "tabular", "sql", "json", "":
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

	dArgs.skinny = apr.Contains(SkinnyFlag)

	f := apr.GetValueOrDefault(FormatFlag, "tabular")
	switch strings.ToLower(f) {
	case "tabular":
		dArgs.diffOutput = TabularDiffOutput
	case "sql":
		dArgs.diffOutput = SQLDiffOutput
	case "json":
		dArgs.diffOutput = JsonDiffOutput
	}

	dArgs.limit, _ = apr.GetInt(limitParam)
	dArgs.where = apr.GetValueOrDefault(whereParam, "")

	tableNames, err := dArgs.applyDiffRoots(ctx, dEnv, apr.Args, apr.Contains(CachedFlag))
	if err != nil {
		return nil, err
	}

	dArgs.tableSet = set.NewStrSet(nil)

	for _, tableName := range tableNames {
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

func diffUserTables(ctx context.Context, dEnv *env.DoltEnv, dArgs *diffArgs) errhand.VerboseError {
	var err error

	tableDeltas, err := diff.GetTableDeltas(ctx, dArgs.fromRoot, dArgs.toRoot)
	if err != nil {
		return errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
	}

	engine, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sort.Slice(tableDeltas, func(i, j int) bool {
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})

	dw, err := newDiffWriter(dArgs.diffOutput)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	for _, td := range tableDeltas {
		verr := diffUserTable(ctx, td, engine, dArgs, dw)
		if verr != nil {
			return verr
		}
	}

	err = dw.Close(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func diffUserTable(
	ctx context.Context,
	td diff.TableDelta,
	engine *engine.SqlEngine,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	if !dArgs.tableSet.Contains(td.FromName) && !dArgs.tableSet.Contains(td.ToName) {
		return nil
	}

	fromTable := td.FromTable
	toTable := td.ToTable

	if fromTable == nil && toTable == nil {
		return errhand.BuildDError("error: both tables in tableDelta are nil").Build()
	}

	err := dw.BeginTable(ctx, td)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	if dArgs.diffParts&Summary != 0 {
		return printDiffSummary(ctx, td, fromSch.GetAllCols().Size(), toSch.GetAllCols().Size())
	}

	if dArgs.diffParts&SchemaOnlyDiff != 0 {
		err := dw.WriteSchemaDiff(ctx, dArgs.toRoot, td)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	if td.IsDrop() && dArgs.diffOutput == SQLDiffOutput {
		return nil // don't output DELETE FROM statements after DROP TABLE
	} else if td.IsAdd() {
		fromSch = toSch
	}

	verr := diffRows(ctx, engine, td, dArgs, dw)
	if verr != nil {
		return verr
	}

	return nil
}

func writeSqlSchemaDiff(ctx context.Context, td diff.TableDelta, toSchemas map[string]schema.Schema) errhand.VerboseError {
	ddlStatements, err := sqlSchemaDiff(ctx, td, toSchemas)
	if err != nil {
		return err
	}

	for _, stmt := range ddlStatements {
		cli.Println(stmt)
	}

	return nil
}

// sqlSchemaDiff returns a slice of DDL statements that will transform the schema in the from delta to the schema in
// the to delta.
// TODO: this doesn't handle constraints or triggers
// TODO: this should live in the diff package
func sqlSchemaDiff(ctx context.Context, td diff.TableDelta, toSchemas map[string]schema.Schema) ([]string, errhand.VerboseError) {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return nil, errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	var ddlStatements []string

	if td.IsDrop() {
		ddlStatements = append(ddlStatements, sqlfmt.DropTableStmt(td.FromName))
	} else if td.IsAdd() {
		sqlDb := sqle.NewSingleTableDatabase(td.ToName, toSch, td.ToFks, td.ToFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		stmt, err := sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		ddlStatements = append(ddlStatements, stmt)
	} else {
		if td.FromName != td.ToName {
			ddlStatements = append(ddlStatements, sqlfmt.RenameTableStmt(td.FromName, td.ToName))
		}

		eq := schema.SchemasAreEqual(fromSch, toSch)
		if eq && !td.HasFKChanges() {
			return ddlStatements, nil
		}

		colDiffs, unionTags := diff.DiffSchColumns(fromSch, toSch)
		for _, tag := range unionTags {
			cd := colDiffs[tag]
			switch cd.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddColStmt(td.ToName, sqlfmt.FmtCol(0, 0, 0, *cd.New)))
			case diff.SchDiffRemoved:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropColStmt(td.ToName, cd.Old.Name))
			case diff.SchDiffModified:
				// Ignore any primary key set changes here
				if cd.Old.IsPartOfPK != cd.New.IsPartOfPK {
					continue
				}
				if cd.Old.Name != cd.New.Name {
					ddlStatements = append(ddlStatements, sqlfmt.AlterTableRenameColStmt(td.ToName, cd.Old.Name, cd.New.Name))
				}
			}
		}

		// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
		if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropPks(td.ToName))
			if toSch.GetPKCols().Size() > 0 {
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddPrimaryKeys(td.ToName, toSch.GetPKCols()))
			}
		}

		for _, idxDiff := range diff.DiffSchIndexes(fromSch, toSch) {
			switch idxDiff.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
			case diff.SchDiffRemoved:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
			case diff.SchDiffModified:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
			}
		}

		for _, fkDiff := range diff.DiffForeignKeys(td.FromFks, td.ToFks) {
			switch fkDiff.DiffType {
			case diff.SchDiffNone:
			case diff.SchDiffAdded:
				parentSch := toSchemas[fkDiff.To.ReferencedTableName]
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
			case diff.SchDiffRemoved:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
			case diff.SchDiffModified:
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))

				parentSch := toSchemas[fkDiff.To.ReferencedTableName]
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
			}
		}
	}

	return ddlStatements, nil
}

func diffRows(
	ctx context.Context,
	se *engine.SqlEngine,
	td diff.TableDelta,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	from, to := dArgs.fromRef, dArgs.toRef

	diffable := schema.ArePrimaryKeySetsDiffable(td.Format(), td.FromSch, td.ToSch)
	canSqlDiff := !(td.ToSch == nil || (td.FromSch != nil && !schema.SchemasAreEqual(td.FromSch, td.ToSch)))

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

	// We always instantiate a RowWriter in case the diffWriter needs it to close off any work from schema output
	rowWriter, err := dw.RowWriter(ctx, td, unionSch)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// can't diff
	if !diffable {
		// TODO: this messes up some structured output if the user didn't redirect it
		cli.PrintErrf("Primary key sets differ between revisions for table %s, skipping data diff\n", td.ToName)
		err := rowWriter.Close(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	} else if dArgs.diffOutput == SQLDiffOutput && !canSqlDiff {
		// TODO: this is overly broad, we can absolutely do better
		_, _ = fmt.Fprintf(cli.CliErr, "Incompatible schema change, skipping data diff\n")
		err := rowWriter.Close(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	// no data diff requested
	if dArgs.diffParts&DataOnlyDiff == 0 {
		err := rowWriter.Close(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	// do the data diff
	tableName := td.ToName
	if len(tableName) == 0 {
		tableName = td.FromName
	}

	columns := getColumnNamesString(td.FromSch, td.ToSch)
	query := fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columns, "diff_type", from, to, tableName)

	if len(dArgs.where) > 0 {
		query += " where " + dArgs.where
	}

	if dArgs.limit >= 0 {
		query += " limit " + strconv.Itoa(dArgs.limit)
	}

	sqlCtx, err := engine.NewLocalSqlContext(ctx, se)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sch, rowIter, err := se.Query(sqlCtx, query)
	if sql.ErrSyntaxError.Is(err) {
		return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", query).AddCause(err).Build()
	} else if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
	}

	defer rowIter.Close(sqlCtx)
	defer rowWriter.Close(ctx)

	var modifiedColNames map[string]bool
	if dArgs.skinny {
		modifiedColNames, err = getModifiedCols(sqlCtx, rowIter, unionSch, sch)
		if err != nil {
			return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
		}

		// instantiate a new schema that only contains the columns with changes
		var filteredUnionSch sql.Schema
		for _, s := range unionSch {
			for colName := range modifiedColNames {
				if s.Name == colName {
					filteredUnionSch = append(filteredUnionSch, s)
				}
			}
		}

		// instantiate a new RowWriter with the new schema that only contains the columns with changes
		rowWriter, err = dw.RowWriter(ctx, td, filteredUnionSch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		defer rowWriter.Close(ctx)

		// reset the row iterator
		err = rowIter.Close(sqlCtx)
		if err != nil {
			return errhand.BuildDError("Error closing row iterator:\n%s", query).AddCause(err).Build()
		}
		_, rowIter, err = se.Query(sqlCtx, query)
		defer rowIter.Close(sqlCtx)
		if sql.ErrSyntaxError.Is(err) {
			return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", query).AddCause(err).Build()
		} else if err != nil {
			return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
		}
	}

	err = writeDiffResults(sqlCtx, sch, unionSch, rowIter, rowWriter, modifiedColNames, dArgs.skinny)
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

func getColumnNamesString(fromSch, toSch schema.Schema) string {
	var cols []string
	if fromSch != nil {
		fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("`from_%s`", col.Name))
			return false, nil
		})
	}
	if toSch != nil {
		toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("`to_%s`", col.Name))
			return false, nil
		})
	}
	return strings.Join(cols, ",")
}

func writeDiffResults(
	ctx *sql.Context,
	diffQuerySch sql.Schema,
	targetSch sql.Schema,
	iter sql.RowIter,
	writer diff.SqlRowDiffWriter,
	modifiedColNames map[string]bool,
	filterChangedCols bool,
) error {
	ds, err := newDiffSplitter(diffQuerySch, targetSch)
	if err != nil {
		return err
	}

	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		oldRow, newRow, err := ds.splitDiffResultRow(r)
		if err != nil {
			return err
		}

		if filterChangedCols {
			var filteredOldRow, filteredNewRow rowDiff
			for i, changeType := range newRow.colDiffs {
				if (changeType == diff.Added|diff.Removed) || modifiedColNames[targetSch[i].Name] {
					if i < len(oldRow.row) {
						filteredOldRow.row = append(filteredOldRow.row, oldRow.row[i])
						filteredOldRow.colDiffs = append(filteredOldRow.colDiffs, oldRow.colDiffs[i])
						filteredOldRow.rowDiff = oldRow.rowDiff
					}

					if i < len(newRow.row) {
						filteredNewRow.row = append(filteredNewRow.row, newRow.row[i])
						filteredNewRow.colDiffs = append(filteredNewRow.colDiffs, newRow.colDiffs[i])
						filteredNewRow.rowDiff = newRow.rowDiff
					}
				}
			}

			oldRow = filteredOldRow
			newRow = filteredNewRow
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
}

// getModifiedCols returns a set of the names of columns that are modified, as well as the name of the primary key for a particular row iterator and schema.
// In the case where rows are added or removed, all columns will be included
// unionSch refers to a joint schema between the schema before and after any schema changes pertaining to the diff,
// while diffQuerySch refers to the schema returned by the "dolt_diff" sql query.
func getModifiedCols(
	ctx *sql.Context,
	iter sql.RowIter,
	unionSch sql.Schema,
	diffQuerySch sql.Schema,
) (map[string]bool, error) {
	modifiedColNames := make(map[string]bool)
	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		ds, err := newDiffSplitter(diffQuerySch, unionSch)
		if err != nil {
			return modifiedColNames, err
		}

		oldRow, newRow, err := ds.splitDiffResultRow(r)
		if err != nil {
			return modifiedColNames, err
		}

		for i, changeType := range newRow.colDiffs {
			if changeType != diff.None || unionSch[i].PrimaryKey {
				modifiedColNames[unionSch[i].Name] = true
			}
		}

		for i, changeType := range oldRow.colDiffs {
			if changeType != diff.None || unionSch[i].PrimaryKey {
				modifiedColNames[unionSch[i].Name] = true
			}
		}
	}

	return modifiedColNames, nil
}
