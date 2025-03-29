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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

type diffOutput int
type diffPart int

const (
	SchemaOnlyDiff diffPart = 1  // 0b0000 0001
	DataOnlyDiff   diffPart = 2  // 0b0000 0010
	NameOnlyDiff   diffPart = 4  // 0b0000 0100
	Stat           diffPart = 8  // 0b0000 1000
	Summary        diffPart = 16 // 0b0001 0000

	SchemaAndDataDiff = SchemaOnlyDiff | DataOnlyDiff

	TabularDiffOutput diffOutput = 1
	SQLDiffOutput     diffOutput = 2
	JsonDiffOutput    diffOutput = 3

	DataFlag     = "data"
	SchemaFlag   = "schema"
	NameOnlyFlag = "name-only"
	StatFlag     = "stat"
	SummaryFlag  = "summary"
	whereParam   = "where"
	limitParam   = "limit"
	SkinnyFlag   = "skinny"
	MergeBase    = "merge-base"
	DiffMode     = "diff-mode"
	ReverseFlag  = "reverse"
)

var diffDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show changes between commits, commit and working tree, etc",
	LongDesc: `
Show changes between the working and staged tables, changes between the working tables and the tables within a commit, or changes between tables at two commits.

{{.EmphasisLeft}}dolt diff [--options] [<tables>...]{{.EmphasisRight}}
   This form is to view the changes you made relative to the staging area for the next commit. In other words, the differences are what you could tell Dolt to further add but you still haven't. You can stage these changes by using dolt add.

{{.EmphasisLeft}}dolt diff [--options] [--merge-base] <commit> [<tables>...]{{.EmphasisRight}}
   This form is to view the changes you have in your working tables relative to the named {{.LessThan}}commit{{.GreaterThan}}. You can use HEAD to compare it with the latest commit, or a branch name to compare with the tip of a different branch. If {{.EmphasisLeft}}--merge-base{{.EmphasisRight}} is given, instead of using {{.LessThan}}commit{{.GreaterThan}}, use the merge base of {{.LessThan}}commit{{.GreaterThan}} and HEAD. {{.EmphasisLeft}}dolt diff --merge-base A{{.EmphasisRight}} is equivalent to {{.EmphasisLeft}}dolt diff $(dolt merge-base A HEAD){{.EmphasisRight}} and {{.EmphasisLeft}}dolt diff A...HEAD{{.EmphasisRight}}.

{{.EmphasisLeft}}dolt diff [--options] [--merge-base] <commit> <commit> [<tables>...]{{.EmphasisRight}}
   This is to view the changes between two arbitrary {{.EmphasisLeft}}commit{{.EmphasisRight}}. If {{.EmphasisLeft}}--merge-base{{.EmphasisRight}} is given, use the merge base of the two commits for the "before" side. {{.EmphasisLeft}}dolt diff --merge-base A B{{.EmphasisRight}} is equivalent to {{.EmphasisLeft}}dolt diff $(dolt merge-base A B) B{{.EmphasisRight}} and {{.EmphasisLeft}}dolt diff A...B{{.EmphasisRight}}.

{{.EmphasisLeft}}dolt diff [--options] <commit>..<commit> [<tables>...]{{.EmphasisRight}}
   This is synonymous to the above form (without the ..) to view the changes between two arbitrary {{.EmphasisLeft}}commit{{.EmphasisRight}}.

{{.EmphasisLeft}}dolt diff [--options] <commit>...<commit> [<tables>...]{{.EmphasisRight}}
   This is to view the changes on the branch containing and up to the second {{.LessThan}}commit{{.GreaterThan}}, starting at a common ancestor of both {{.LessThan}}commit{{.GreaterThan}}. {{.EmphasisLeft}}dolt diff A...B{{.EmphasisRight}} is equivalent to {{.EmphasisLeft}}dolt diff $(dolt merge-base A B) B{{.EmphasisRight}} and {{.EmphasisLeft}}dolt diff --merge-base A B{{.EmphasisRight}}. You can omit any one of {{.LessThan}}commit{{.GreaterThan}}, which has the same effect as using HEAD instead.

The diffs displayed can be limited to show the first N by providing the parameter {{.EmphasisLeft}}--limit N{{.EmphasisRight}} where {{.EmphasisLeft}}N{{.EmphasisRight}} is the number of diffs to display.

To filter which data rows are displayed, use {{.EmphasisLeft}}--where <SQL expression>{{.EmphasisRight}}. Table column names in the filter expression must be prefixed with {{.EmphasisLeft}}from_{{.EmphasisRight}} or {{.EmphasisLeft}}to_{{.EmphasisRight}}, e.g. {{.EmphasisLeft}}to_COLUMN_NAME > 100{{.EmphasisRight}} or {{.EmphasisLeft}}from_COLUMN_NAME + to_COLUMN_NAME = 0{{.EmphasisRight}}.

The {{.EmphasisLeft}}--diff-mode{{.EmphasisRight}} argument controls how modified rows are presented when the format output is set to {{.EmphasisLeft}}tabular{{.EmphasisRight}}. When set to {{.EmphasisLeft}}row{{.EmphasisRight}}, modified rows are presented as old and new rows. When set to {{.EmphasisLeft}}line{{.EmphasisRight}}, modified rows are presented as a single row, and changes are presented using "+" and "-" within the column. When set to {{.EmphasisLeft}}in-place{{.EmphasisRight}}, modified rows are presented as a single row, and changes are presented side-by-side with a color distinction (requires a color-enabled terminal). When set to {{.EmphasisLeft}}context{{.EmphasisRight}}, rows that contain at least one column that spans multiple lines uses {{.EmphasisLeft}}line{{.EmphasisRight}}, while all other rows use {{.EmphasisLeft}}row{{.EmphasisRight}}. The default value is {{.EmphasisLeft}}context{{.EmphasisRight}}.
`,
	Synopsis: []string{
		`[options] [{{.LessThan}}commit{{.GreaterThan}}] [{{.LessThan}}tables{{.GreaterThan}}...]`,
		`[options] {{.LessThan}}commit{{.GreaterThan}} {{.LessThan}}commit{{.GreaterThan}} [{{.LessThan}}tables{{.GreaterThan}}...]`,
	},
}

type diffDisplaySettings struct {
	diffParts  diffPart
	diffOutput diffOutput
	diffMode   diff.Mode
	limit      int
	where      string
	skinny     bool
}

type diffDatasets struct {
	fromRef string
	toRef   string
}

type diffArgs struct {
	*diffDisplaySettings
	*diffDatasets
	tableSet *set.StrSet
}

type diffStatistics struct {
	TableName      string
	RowsUnmodified uint64
	RowsAdded      uint64
	RowsDeleted    uint64
	RowsModified   uint64
	CellsAdded     uint64
	CellsDeleted   uint64
	CellsModified  uint64
	OldRowCount    uint64
	NewRowCount    uint64
	OldCellCount   uint64
	NewCellCount   uint64
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
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.SupportsFlag(DataFlag, "d", "Show only the data changes, do not show the schema changes (Both shown by default).")
	ap.SupportsFlag(SchemaFlag, "s", "Show only the schema changes, do not show the data changes (Both shown by default).")
	ap.SupportsFlag(StatFlag, "", "Show stats of data changes")
	ap.SupportsFlag(SummaryFlag, "", "Show summary of data and schema changes")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format diff output. Valid values are tabular, sql, json. Defaults to tabular.")
	ap.SupportsString(whereParam, "", "column", "filters columns based on values in the diff.  See {{.EmphasisLeft}}dolt diff --help{{.EmphasisRight}} for details.")
	ap.SupportsInt(limitParam, "", "record_count", "limits to the first N diffs.")
	ap.SupportsFlag(cli.StagedFlag, "", "Show only the staged data changes.")
	ap.SupportsFlag(cli.CachedFlag, "c", "Synonym for --staged")
	ap.SupportsFlag(SkinnyFlag, "sk", "Shows only primary key columns and any columns with data changes.")
	ap.SupportsFlag(MergeBase, "", "Uses merge base of the first commit and second commit (or HEAD if not supplied) as the first commit")
	ap.SupportsString(DiffMode, "", "diff mode", "Determines how to display modified rows with tabular output. Valid values are row, line, in-place, context. Defaults to context.")
	ap.SupportsFlag(ReverseFlag, "R", "Reverses the direction of the diff.")
	ap.SupportsFlag(NameOnlyFlag, "", "Only shows table names.")
	return ap
}

func (cmd DiffCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd DiffCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, usage, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, diffDocs)
	if terminate {
		return status
	}

	verr := cmd.validateArgs(apr)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	queryist, oldSqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	sqlCtx := doltdb.ContextWithDoltCICreateBypassKey(oldSqlCtx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	dArgs, err := parseDiffArgs(queryist, sqlCtx, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	verr = diffUserTables(queryist, sqlCtx, dArgs)
	return HandleVErrAndExitCode(verr, usage)
}

func (cmd DiffCmd) validateArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.Contains(StatFlag) || apr.Contains(SummaryFlag) {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) {
			return errhand.BuildDError("invalid Arguments: --stat and --summary cannot be combined with --schema or --data").Build()
		}
	}

	if apr.Contains(NameOnlyFlag) {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) || apr.Contains(StatFlag) || apr.Contains(SummaryFlag) {
			return errhand.BuildDError("invalid Arguments: --name-only cannot be combined with --schema, --data, --stat, or --summary").Build()
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

func parseDiffDisplaySettings(apr *argparser.ArgParseResults) *diffDisplaySettings {
	displaySettings := &diffDisplaySettings{}

	displaySettings.diffParts = SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		displaySettings.diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		displaySettings.diffParts = SchemaOnlyDiff
	} else if apr.Contains(StatFlag) {
		displaySettings.diffParts = Stat
	} else if apr.Contains(SummaryFlag) {
		displaySettings.diffParts = Summary
	} else if apr.Contains(NameOnlyFlag) {
		displaySettings.diffParts = NameOnlyDiff
	}

	displaySettings.skinny = apr.Contains(SkinnyFlag)

	f := apr.GetValueOrDefault(FormatFlag, "tabular")
	switch strings.ToLower(f) {
	case "tabular":
		displaySettings.diffOutput = TabularDiffOutput
		switch strings.ToLower(apr.GetValueOrDefault(DiffMode, "context")) {
		case "row":
			displaySettings.diffMode = diff.ModeRow
		case "line":
			displaySettings.diffMode = diff.ModeLine
		case "in-place":
			displaySettings.diffMode = diff.ModeInPlace
		case "context":
			displaySettings.diffMode = diff.ModeContext
		}
	case "sql":
		displaySettings.diffOutput = SQLDiffOutput
	case "json":
		displaySettings.diffOutput = JsonDiffOutput
	}

	displaySettings.limit, _ = apr.GetInt(limitParam)
	displaySettings.where = apr.GetValueOrDefault(whereParam, "")

	return displaySettings
}

func parseDiffArgs(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) (*diffArgs, error) {
	dArgs := &diffArgs{
		diffDisplaySettings: parseDiffDisplaySettings(apr),
	}

	staged := apr.Contains(cli.StagedFlag) || apr.Contains(cli.CachedFlag)

	tableNames, err := dArgs.applyDiffRoots(queryist, sqlCtx, apr.Args, staged, apr.Contains(MergeBase))
	if err != nil {
		return nil, err
	}

	if apr.Contains(ReverseFlag) {
		dArgs.diffDatasets = &diffDatasets{
			fromRef: dArgs.toRef,
			toRef:   dArgs.fromRef,
		}
	}

	tableSet, err := parseDiffTableSetSql(queryist, sqlCtx, dArgs.diffDatasets, tableNames)
	if err != nil {
		return nil, err
	}

	dArgs.tableSet = tableSet

	return dArgs, nil
}

func parseDiffTableSetSql(queryist cli.Queryist, sqlCtx *sql.Context, datasets *diffDatasets, tableNames []string) (*set.StrSet, error) {

	tablesAtFromRef, err := getTableNamesAtRef(queryist, sqlCtx, datasets.fromRef)
	if err != nil {
		return nil, err
	}
	tablesAtToRef, err := getTableNamesAtRef(queryist, sqlCtx, datasets.toRef)
	if err != nil {
		return nil, err
	}
	tableSet := set.NewStrSet(nil)

	for _, tableName := range tableNames {
		// verify table args exist in at least one root
		_, ok := tablesAtFromRef[tableName]
		if ok {
			tableSet.Add(tableName)
			continue
		}

		_, ok = tablesAtToRef[tableName]
		if ok {
			tableSet.Add(tableName)
			continue
		}

		return nil, fmt.Errorf("table %s does not exist in either revision", tableName)
	}

	// if no tables or docs were specified as args, diff all tables and docs
	if len(tableNames) == 0 {
		seenTableNames := make(map[string]bool)
		for _, tables := range []map[string]bool{tablesAtFromRef, tablesAtToRef} {
			for tableName := range tables {
				if _, ok := seenTableNames[tableName]; !ok {
					seenTableNames[tableName] = true
					tableSet.Add(tableName)
				}
			}
		}
	}

	return tableSet, nil
}

var doltSystemTables = []string{
	"dolt_procedures",
	"dolt_schemas",
}

func getTableNamesAtRef(queryist cli.Queryist, sqlCtx *sql.Context, ref string) (map[string]bool, error) {
	// query for user-created tables
	q, err := dbr.InterpolateForDialect("SHOW FULL TABLES AS OF ?", []interface{}{ref}, dialect.MySQL)
	if err != nil {
		return nil, fmt.Errorf("error interpolating query: %w", err)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	tableNames := make(map[string]bool)
	for _, row := range rows {
		tableName, ok, err := sql.Unwrap[string](sqlCtx, row[0])
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("unexpected type for table name, expected string, found %T", row[0])
		}
		tableType, ok, err := sql.Unwrap[string](sqlCtx, row[1])
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("unexpected type for table type, expected string, found %T", row[1])
		}
		isTable := tableType == "BASE TABLE"
		if isTable {
			tableNames[tableName] = true
		}
	}

	// add system tables, if they exist at this ref
	for _, sysTable := range doltSystemTables {
		interpolatedQuery, err := dbr.InterpolateForDialect("SHOW CREATE TABLE ? AS OF ?", []interface{}{dbr.I(sysTable), ref}, dialect.MySQL)
		if err != nil {
			return nil, fmt.Errorf("error interpolating query: %w", err)
		}
		result, err := GetRowsForSql(queryist, sqlCtx, interpolatedQuery)
		if err != nil {
			return nil, fmt.Errorf("error getting system table %s: %w", sysTable, err)
		}

		if len(result) > 0 {
			tableNames[sysTable] = true
		}
	}

	return tableNames, nil
}

func isTableNotFoundError(err error) bool {
	if sql.ErrTableNotFound.Is(err) {
		return true
	}
	mse, ok := err.(*mysql.MySQLError)
	if ok {
		if strings.HasPrefix(mse.Message, "table not found:") {
			return true
		}
	}

	return false
}

// applyDiffRoots applies the appropriate |from| and |to| root values to the receiver and returns the table names
// (if any) given to the command.
func (dArgs *diffArgs) applyDiffRoots(queryist cli.Queryist, sqlCtx *sql.Context, args []string, isCached, useMergeBase bool) ([]string, error) {
	dArgs.diffDatasets = &diffDatasets{
		fromRef: doltdb.Staged,
		toRef:   doltdb.Working,
	}

	if isCached {
		dArgs.fromRef = "HEAD"
		dArgs.toRef = doltdb.Staged
	}

	if len(args) == 0 {
		if useMergeBase {
			return nil, errors.New("Must supply at least one revision when using --merge-base flag")
		}
		// `dolt diff`
		return nil, nil
	}

	if strings.Contains(args[0], "..") {
		if useMergeBase {
			return nil, errors.New("Cannot use `..` or `...` with --merge-base flag")
		}
		err := dArgs.applyDotRevisions(queryist, sqlCtx, args)
		if err != nil {
			return nil, err
		}
		return args[1:], err
	}

	fromRef := args[0]
	// treat the first arg as a ref spec
	_, err := getTableNamesAtRef(queryist, sqlCtx, fromRef)
	if errors.Is(err, doltdb.ErrGhostCommitEncountered) {
		return nil, err
	}
	// if it doesn't resolve, treat it as a table name
	if err != nil {
		// `dolt diff table`
		if useMergeBase {
			return nil, errors.New("Must supply at least one revision when using --merge-base flag")
		}
		return args, nil
	}
	dArgs.fromRef = fromRef

	if len(args) == 1 {
		// `dolt diff from_commit`
		if useMergeBase {
			err := dArgs.applyMergeBase(queryist, sqlCtx, args[0], "HEAD")
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	toRef := args[1]
	// treat the first arg as a ref spec
	_, err = getTableNamesAtRef(queryist, sqlCtx, toRef)
	// if it doesn't resolve, treat it as a table name
	if err != nil {
		// `dolt diff from_commit [...tables]`
		if useMergeBase {
			err := dArgs.applyMergeBase(queryist, sqlCtx, args[0], "HEAD")
			if err != nil {
				return nil, err
			}
		}
		return args[1:], nil
	}
	dArgs.toRef = toRef

	if useMergeBase {
		err := dArgs.applyMergeBase(queryist, sqlCtx, args[0], args[1])
		if err != nil {
			return nil, err
		}
	}

	// `dolt diff from_commit to_commit [...tables]`
	return args[2:], nil
}

// applyMergeBase applies the merge base of two revisions to the |from| root
// values.
func (dArgs *diffArgs) applyMergeBase(queryist cli.Queryist, sqlCtx *sql.Context, leftStr, rightStr string) error {
	mergeBaseStr, err := getCommonAncestor(queryist, sqlCtx, leftStr, rightStr)
	if err != nil {
		return err
	}

	dArgs.fromRef = mergeBaseStr

	return nil
}

func getCommonAncestor(queryist cli.Queryist, sqlCtx *sql.Context, c1, c2 string) (string, error) {
	q, err := dbr.InterpolateForDialect("select dolt_merge_base(?, ?)", []interface{}{c1, c2}, dialect.MySQL)
	if err != nil {
		return "", fmt.Errorf("error interpolating query: %w", err)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", errors.New("unexpected number of rows returned from dolt_merge_base")
	}
	ancestor := rows[0][0].(string)
	return ancestor, nil
}

// applyDotRevisions applies the appropriate |from| and |to| root values to the
// receiver for arguments containing `..` or `...`
func (dArgs *diffArgs) applyDotRevisions(queryist cli.Queryist, sqlCtx *sql.Context, args []string) error {
	// `dolt diff from_commit...to_commit [...tables]`
	if strings.Contains(args[0], "...") {
		refs := strings.Split(args[0], "...")

		if len(refs[0]) > 0 {
			right := refs[1]
			// Use current HEAD if right side of `...` does not exist
			if len(refs[1]) == 0 {
				right = "HEAD"
			}

			err := dArgs.applyMergeBase(queryist, sqlCtx, refs[0], right)
			if err != nil {
				return err
			}
		}

		if len(refs[1]) > 0 {
			dArgs.toRef = refs[1]
		}

		return nil
	}

	// `dolt diff from_commit..to_commit [...tables]`
	if strings.Contains(args[0], "..") {
		refs := strings.Split(args[0], "..")

		if len(refs[0]) > 0 {
			dArgs.fromRef = refs[0]
		}

		if len(refs[1]) > 0 {
			dArgs.toRef = refs[1]
		}

		return nil
	}

	return nil
}

var diffSummarySchema = sql.Schema{
	&sql.Column{Name: "Table name", Type: types.Text, Nullable: false},
	&sql.Column{Name: "Diff type", Type: types.Text, Nullable: false},
	&sql.Column{Name: "Data change", Type: types.Boolean, Nullable: false},
	&sql.Column{Name: "Schema change", Type: types.Boolean, Nullable: false},
}

func printDiffSummary(ctx context.Context, diffSummaries []diff.TableDeltaSummary, dArgs *diffArgs) errhand.VerboseError {
	cliWR := iohelp.NopWrCloser(cli.OutStream)
	wr := tabular.NewFixedWidthTableWriter(diffSummarySchema, cliWR, 100)
	defer wr.Close(ctx)

	for _, diffSummary := range diffSummaries {

		// TODO: schema name
		shouldPrintTables := dArgs.tableSet.Contains(diffSummary.FromTableName.Name) || dArgs.tableSet.Contains(diffSummary.ToTableName.Name)
		if !shouldPrintTables {
			return nil
		}

		tableName := diffSummary.TableName.Name
		if diffSummary.DiffType == "renamed" {
			tableName = fmt.Sprintf("%s -> %s", diffSummary.FromTableName.Name, diffSummary.ToTableName.Name)
		}
		err := wr.WriteSqlRow(ctx, sql.Row{tableName, diffSummary.DiffType, diffSummary.DataChange, diffSummary.SchemaChange})
		if err != nil {
			return errhand.BuildDError("could not write table delta summary").AddCause(err).Build()
		}

	}

	return nil
}

func getDeltasBetweenRefs(queryist cli.Queryist, sqlCtx *sql.Context, fromRef, toRef string) ([]diff.TableDeltaSummary, error) {
	diffSummaries, err := getDiffSummariesBetweenRefs(queryist, sqlCtx, fromRef, toRef)
	if err != nil {
		return nil, err
	}

	schemaSummaries, err := getSchemaDiffSummariesBetweenRefs(queryist, sqlCtx, fromRef, toRef)
	if err != nil {
		return nil, err
	}

	allSummaries := []diff.TableDeltaSummary{}
	allSummaries = append(allSummaries, diffSummaries...)

	for _, schemaSummary := range schemaSummaries {
		deltaExists := false
		for i, summary := range allSummaries {
			deltaExists = summary.FromTableName == schemaSummary.FromTableName && summary.ToTableName == schemaSummary.ToTableName
			if deltaExists {
				existingSummary := allSummaries[i]
				existingSummary.SchemaChange = true
				existingSummary.AlterStmts = append(existingSummary.AlterStmts, schemaSummary.AlterStmts...)
				allSummaries[i] = existingSummary
				break
			}
		}
		if !deltaExists {
			allSummaries = append(allSummaries, schemaSummary)
		}
	}

	return allSummaries, nil
}

func getSchemaDiffSummariesBetweenRefs(queryist cli.Queryist, sqlCtx *sql.Context, fromRef, toRef string) ([]diff.TableDeltaSummary, error) {
	q, err := dbr.InterpolateForDialect("select * from dolt_schema_diff(?, ?)", []interface{}{fromRef, toRef}, dialect.MySQL)
	if err != nil {
		return nil, fmt.Errorf("error: unable to interpolate query: %w", err)
	}
	schemaDiffRows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, fmt.Errorf("error: unable to get schema diff from %s to %s: %w", fromRef, toRef, err)
	}

	var summaries []diff.TableDeltaSummary
	for _, row := range schemaDiffRows {
		fromTable := row[0].(string)
		toTable := row[1].(string)
		fromCreateStmt := row[2].(string)
		toCreateStmt := row[3].(string)
		var diffType = ""
		var tableName = ""
		switch {
		case fromTable == toTable:
			if fromCreateStmt != toCreateStmt {
				diffType = "modified"
				tableName = fromTable
			}
		case fromTable == "":
			diffType = "added"
			tableName = toTable
		case toTable == "":
			diffType = "dropped"
			tableName = fromTable
		case fromTable != "" && toTable != "" && fromTable != toTable:
			diffType = "renamed"
			tableName = toTable
		default:
			return nil, fmt.Errorf("error: unexpected schema diff case: fromTable='%s', toTable='%s'", fromTable, toTable)
		}

		q, err := dbr.InterpolateForDialect(
			"select statement_order, statement from dolt_patch(?, ?) where diff_type='schema' and (table_name=? or table_name=?) order by statement_order asc",
			[]interface{}{fromRef, toRef, fromTable, toTable},
			dialect.MySQL)
		if err != nil {
			return nil, fmt.Errorf("error: unable to interpolate dolt_patch query: %w", err)
		}
		patchRows, err := GetRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return nil, fmt.Errorf("error: unable to get dolt_patch rows from %s to %s: %w", fromRef, toRef, err)
		}
		alterStmts := []string{}
		for _, row := range patchRows {
			alterStmts = append(alterStmts, row[1].(string))
		}

		summary := diff.TableDeltaSummary{
			TableName:     doltdb.TableName{Name: tableName},
			FromTableName: doltdb.TableName{Name: fromTable},
			ToTableName:   doltdb.TableName{Name: toTable},
			DiffType:      diffType,
			DataChange:    false,
			SchemaChange:  true,
			AlterStmts:    alterStmts,
		}

		summaries = append(summaries, summary)
	}

	return summaries, nil
}

func getDiffSummariesBetweenRefs(queryist cli.Queryist, sqlCtx *sql.Context, fromRef, toRef string) ([]diff.TableDeltaSummary, error) {
	q, err := dbr.InterpolateForDialect("select * from dolt_diff_summary(?, ?)", []interface{}{fromRef, toRef}, dialect.MySQL)
	dataDiffRows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, fmt.Errorf("error: unable to get diff summary from %s to %s: %w", fromRef, toRef, err)
	}

	summaries := []diff.TableDeltaSummary{}

	for _, row := range dataDiffRows {
		summary := diff.TableDeltaSummary{}
		summary.FromTableName.Name = row[0].(string)
		summary.ToTableName.Name = row[1].(string)
		summary.DiffType = row[2].(string)
		summary.DataChange, err = GetTinyIntColAsBool(row[3])
		if err != nil {
			return nil, fmt.Errorf("error: unable to parse data change value '%s': %w", row[3], err)
		}
		summary.SchemaChange, err = GetTinyIntColAsBool(row[4])
		if err != nil {
			return nil, fmt.Errorf("error: unable to parse schema change value '%s': %w", row[4], err)
		}

		switch summary.DiffType {
		case "dropped":
			summary.TableName = summary.FromTableName
		case "added":
			summary.TableName = summary.ToTableName
		case "renamed":
			summary.TableName = summary.ToTableName
		case "modified":
			summary.TableName = summary.FromTableName
		default:
			return nil, fmt.Errorf("error: unexpected diff type '%s'", summary.DiffType)
		}

		summaries = append(summaries, summary)
	}

	return summaries, nil
}

func diffUserTables(queryist cli.Queryist, sqlCtx *sql.Context, dArgs *diffArgs) errhand.VerboseError {
	var err error

	deltas, err := getDeltasBetweenRefs(queryist, sqlCtx, dArgs.fromRef, dArgs.toRef)
	if err != nil {
		return errhand.BuildDError("error: unable to get diff summary").AddCause(err).Build()
	}

	if dArgs.diffParts&Summary != 0 {
		return printDiffSummary(sqlCtx, deltas, dArgs)
	}

	dw, err := newDiffWriter(dArgs.diffOutput)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	ignoredTablePatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return errhand.VerboseErrorFromError(fmt.Errorf("couldn't get ignored table patterns, cause: %w", err))
	}

	doltSchemasChanged := false
	for _, delta := range deltas {
		if doltdb.IsFullTextTable(delta.TableName.Name) {
			continue
		}

		// Don't print tables if one side of the diff is an ignored table in the working set being added.
		if len(delta.FromTableName.Name) == 0 {
			ignoreResult, err := ignoredTablePatterns.IsTableNameIgnored(delta.ToTableName)
			if err != nil {
				return errhand.VerboseErrorFromError(err)
			}
			if ignoreResult == doltdb.Ignore {
				continue
			}
		}

		if len(delta.ToTableName.Name) == 0 {
			ignoreResult, err := ignoredTablePatterns.IsTableNameIgnored(delta.FromTableName)
			if err != nil {
				return errhand.VerboseErrorFromError(err)
			}
			if ignoreResult == doltdb.Ignore {
				continue
			}
		}

		if !shouldPrintTableDelta(dArgs.tableSet, delta.ToTableName.Name, delta.FromTableName.Name) {
			continue
		}

		if strings.HasPrefix(delta.ToTableName.Name, diff.DBPrefix) {
			verr := diffDatabase(queryist, sqlCtx, delta, dArgs, dw)
			if verr != nil {
				return verr
			}
			continue
		}

		if isDoltSchemasTable(delta.ToTableName.Name, delta.FromTableName.Name) {
			// save dolt_schemas table diff for last in diff output
			doltSchemasChanged = true
		} else {
			verr := diffUserTable(queryist, sqlCtx, delta, dArgs, dw)
			if verr != nil {
				return verr
			}
		}
	}

	if doltSchemasChanged {
		verr := diffDoltSchemasTable(queryist, sqlCtx, dArgs, dw)
		if verr != nil {
			return verr
		}
	}

	err = dw.Close(sqlCtx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func shouldPrintTableDelta(tablesToPrint *set.StrSet, toTableName, fromTableName string) bool {
	// TODO: this should be case insensitive
	return tablesToPrint.Contains(fromTableName) ||
		tablesToPrint.Contains(toTableName) ||
		strings.HasPrefix(fromTableName, diff.DBPrefix) ||
		strings.HasPrefix(toTableName, diff.DBPrefix)
}

func isDoltSchemasTable(toTableName, fromTableName string) bool {
	return fromTableName == doltdb.SchemasTableName || toTableName == doltdb.SchemasTableName
}

func getTableInfoAtRef(queryist cli.Queryist, sqlCtx *sql.Context, tableName string, ref string) (diff.TableInfo, error) {
	sch, createStmt, err := getTableSchemaAtRef(queryist, sqlCtx, tableName, ref)
	if err != nil {
		return diff.TableInfo{}, fmt.Errorf("error: unable to get schema for table '%s': %w", tableName, err)
	}

	tableInfo := diff.TableInfo{
		Name:       tableName,
		Sch:        sch,
		CreateStmt: createStmt,
	}
	return tableInfo, nil
}

func getTableSchemaAtRef(queryist cli.Queryist, sqlCtx *sql.Context, tableName string, ref string) (sch schema.Schema, createStmt string, err error) {
	var rows []sql.Row
	interpolatedQuery, err := dbr.InterpolateForDialect("SHOW CREATE TABLE ? AS OF ?", []interface{}{dbr.I(tableName), ref}, dialect.MySQL)
	if err != nil {
		return sch, createStmt, fmt.Errorf("error interpolating query: %w", err)
	}
	rows, err = GetRowsForSql(queryist, sqlCtx, interpolatedQuery)
	if err != nil {
		return sch, createStmt, fmt.Errorf("error: unable to get create table statement for table '%s': %w", tableName, err)
	}

	if len(rows) != 1 {
		return sch, createStmt, fmt.Errorf("creating schema, expected 1 row, got %d", len(rows))
	}
	createStmt = rows[0][1].(string)

	// append ; at the end, if one isn't there yet
	if createStmt[len(createStmt)-1] != ';' {
		createStmt += ";"
	}

	sch, err = schemaFromCreateTableStmt(createStmt)
	if err != nil {
		return sch, createStmt, err
	}

	return sch, createStmt, nil
}

func getDatabaseInfoAtRef(queryist cli.Queryist, sqlCtx *sql.Context, tableName string, ref string) (diff.TableInfo, error) {
	createStmt, err := getDatabaseSchemaAtRef(queryist, sqlCtx, tableName, ref)
	if err != nil {
		return diff.TableInfo{}, fmt.Errorf("error: unable to get schema for table '%s': %w", tableName, err)
	}

	tableInfo := diff.TableInfo{
		Name:       tableName,
		Sch:        schema.EmptySchema,
		CreateStmt: createStmt,
	}
	return tableInfo, nil
}

func getDatabaseSchemaAtRef(queryist cli.Queryist, sqlCtx *sql.Context, tableName string, ref string) (string, error) {
	var rows []sql.Row
	// TODO: implement `show create database as of ...`
	tableName = strings.TrimPrefix(tableName, diff.DBPrefix)
	interpolatedQuery, err := dbr.InterpolateForDialect("SHOW CREATE DATABASE ?", []interface{}{dbr.I(tableName)}, dialect.MySQL)
	if err != nil {
		return "", fmt.Errorf("error interpolating query: %w", err)
	}
	rows, err = GetRowsForSql(queryist, sqlCtx, interpolatedQuery)
	if err != nil {
		return "", fmt.Errorf("error: unable to get create database statement for database '%s': %w", tableName, err)
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("creating schema, expected 1 row, got %d", len(rows))
	}
	createStmt := rows[0][1].(string)

	// append ; at the end, if one isn't there yet
	if createStmt[len(createStmt)-1] != ';' {
		createStmt += ";"
	}

	return createStmt, nil
}

// schemaFromCreateTableStmt returns a schema for the CREATE TABLE statement given
// TODO: this is substantially incorrect, doesn't handle primary key ordering, probably other things too
func schemaFromCreateTableStmt(createTableStmt string) (schema.Schema, error) {
	parsed, err := ast.Parse(createTableStmt)
	if err != nil {
		return nil, err
	}
	create, ok := parsed.(*ast.DDL)
	if !ok {
		return nil, fmt.Errorf("expected create table, found %T", parsed)
	}

	primaryCols := make(map[string]bool)
	for _, index := range create.TableSpec.Indexes {
		if index.Info.Primary {
			for _, indexCol := range index.Columns {
				primaryCols[indexCol.Column.Lowered()] = true
			}
			break
		}
	}

	var cols []schema.Column
	for _, col := range create.TableSpec.Columns {
		internalTyp, err := types.ColumnTypeToType(&col.Type)
		typeInfo, err := typeinfo.FromSqlType(internalTyp)
		if err != nil {
			return nil, err
		}

		defBuf := ast.NewTrackedBuffer(nil)
		if col.Type.Default != nil {
			col.Type.Default.Format(defBuf)
		}

		genBuf := ast.NewTrackedBuffer(nil)
		if col.Type.GeneratedExpr != nil {
			col.Type.GeneratedExpr.Format(genBuf)
		}

		onUpBuf := ast.NewTrackedBuffer(nil)
		if col.Type.OnUpdate != nil {
			col.Type.OnUpdate.Format(onUpBuf)
		}

		var comment string
		if col.Type.Comment != nil {
			comment = col.Type.Comment.String()
		}

		sCol := schema.Column{
			Name:          col.Name.String(),
			Kind:          typeInfo.NomsKind(),
			IsPartOfPK:    primaryCols[col.Name.Lowered()],
			TypeInfo:      typeInfo,
			Default:       defBuf.String(),
			Generated:     "",    // TODO
			OnUpdate:      "",    // TODO
			Virtual:       false, // TODO
			AutoIncrement: col.Type.Autoincrement == true,
			Comment:       comment,
		}
		cols = append(cols, sCol)
	}

	sch, err := schema.NewSchema(schema.NewColCollection(cols...), nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return nil, err
	}

	return sch, err
}

func getTableDiffStats(queryist cli.Queryist, sqlCtx *sql.Context, tableName, fromRef, toRef string) ([]diffStatistics, error) {
	q, err := dbr.InterpolateForDialect("select * from dolt_diff_stat(?, ?, ?)", []interface{}{fromRef, toRef, tableName}, dialect.MySQL)
	if err != nil {
		return nil, fmt.Errorf("error interpolating query: %w", err)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, fmt.Errorf("error running diff stats query: %w", err)
	}

	allStats := []diffStatistics{}
	for _, row := range rows {
		rowsUnmodified, err := coallesceNilToUint64(row[1])
		if err != nil {
			return nil, err
		}
		rowsAdded, err := coallesceNilToUint64(row[2])
		if err != nil {
			return nil, err
		}
		rowsDeleted, err := coallesceNilToUint64(row[3])
		if err != nil {
			return nil, err
		}
		rowsModified, err := coallesceNilToUint64(row[4])
		if err != nil {
			return nil, err
		}
		cellsAdded, err := coallesceNilToUint64(row[5])
		if err != nil {
			return nil, err
		}
		cellsDeleted, err := coallesceNilToUint64(row[6])
		if err != nil {
			return nil, err
		}
		cellsModified, err := coallesceNilToUint64(row[7])
		if err != nil {
			return nil, err
		}
		oldRowCount, err := coallesceNilToUint64(row[8])
		if err != nil {
			return nil, err
		}
		newRowCount, err := coallesceNilToUint64(row[9])
		if err != nil {
			return nil, err
		}
		oldCellCount, err := coallesceNilToUint64(row[10])
		if err != nil {
			return nil, err
		}
		newCellCount, err := coallesceNilToUint64(row[11])
		if err != nil {
			return nil, err
		}

		stats := diffStatistics{
			TableName:      row[0].(string),
			RowsUnmodified: rowsUnmodified,
			RowsAdded:      rowsAdded,
			RowsDeleted:    rowsDeleted,
			RowsModified:   rowsModified,
			CellsAdded:     cellsAdded,
			CellsDeleted:   cellsDeleted,
			CellsModified:  cellsModified,
			OldRowCount:    oldRowCount,
			NewRowCount:    newRowCount,
			OldCellCount:   oldCellCount,
			NewCellCount:   newCellCount,
		}
		allStats = append(allStats, stats)
	}
	return allStats, nil
}

func coallesceNilToUint64(val interface{}) (uint64, error) {
	if val == nil {
		return 0, nil
	}
	return getUint64ColAsUint64(val)
}

func diffUserTable(
	queryist cli.Queryist,
	sqlCtx *sql.Context,
	tableSummary diff.TableDeltaSummary,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	fromTable := tableSummary.FromTableName
	toTable := tableSummary.ToTableName

	if dArgs.diffParts&NameOnlyDiff == 0 {
		// TODO: schema names
		err := dw.BeginTable(tableSummary.FromTableName.Name, tableSummary.ToTableName.Name, tableSummary.IsAdd(), tableSummary.IsDrop())
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	var fromTableInfo, toTableInfo *diff.TableInfo

	from, err := getTableInfoAtRef(queryist, sqlCtx, fromTable.Name, dArgs.fromRef)
	if err == nil {
		fromTableInfo = &from
	}
	to, err := getTableInfoAtRef(queryist, sqlCtx, toTable.Name, dArgs.toRef)
	if err == nil {
		toTableInfo = &to
	}

	tableName := fromTable
	if tableName.Name == "" {
		tableName = toTable
	}

	if dArgs.diffParts&NameOnlyDiff != 0 {
		cli.Println(tableName)
		return errhand.VerboseErrorFromError(nil)
	}

	if dArgs.diffParts&Stat != 0 {
		var areTablesKeyless = false

		var fromColLen = 0
		var fromKeyless = false
		if fromTableInfo != nil {
			fromKeyless = schema.IsKeyless(fromTableInfo.Sch)
			fromColLen = fromTableInfo.Sch.GetAllCols().Size()
		}
		var toColLen = 0
		var toKeyless = false
		if toTableInfo != nil {
			toKeyless = schema.IsKeyless(toTableInfo.Sch)
			toColLen = toTableInfo.Sch.GetAllCols().Size()
		}

		// nil table is neither keyless nor keyed
		if fromTableInfo == nil {
			areTablesKeyless = toKeyless
		} else if toTableInfo == nil {
			areTablesKeyless = fromKeyless
		} else {
			if fromKeyless && toKeyless {
				areTablesKeyless = true
			} else if !fromKeyless && !toKeyless {
				areTablesKeyless = false
			} else {
				return errhand.BuildDError("mismatched keyless and keyed schemas for table %s", tableName).Build()
			}
		}

		var diffStats []diffStatistics
		diffStats, err = getTableDiffStats(queryist, sqlCtx, tableName.Name, dArgs.fromRef, dArgs.toRef)
		if err != nil {
			return errhand.BuildDError("cannot retrieve diff stats between '%s' and '%s'", dArgs.fromRef, dArgs.toRef).AddCause(err).Build()
		}

		err = dw.WriteTableDiffStats(diffStats, fromColLen, toColLen, areTablesKeyless)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	if dArgs.diffParts&SchemaOnlyDiff != 0 {
		err = dw.WriteTableSchemaDiff(fromTableInfo, toTableInfo, tableSummary)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	if tableSummary.IsDrop() && dArgs.diffOutput == SQLDiffOutput {
		return nil // don't output DELETE FROM statements after DROP TABLE
	}

	verr := diffRows(queryist, sqlCtx, tableSummary, fromTableInfo, toTableInfo, dArgs, dw)
	if verr != nil {
		return verr
	}

	return nil
}

func diffDoltSchemasTable(
	queryist cli.Queryist,
	sqlCtx *sql.Context,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	query, err := dbr.InterpolateForDialect("select from_name,to_name,from_type,to_type,from_fragment,to_fragment "+
		"from dolt_diff(?, ?, ?) "+
		"order by coalesce(from_type, to_type), coalesce(from_name, to_name)",
		[]interface{}{dArgs.fromRef, dArgs.toRef, doltdb.SchemasTableName}, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("Error building diff query").AddCause(err).Build()
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, query)
	if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
	}

	defer rowIter.Close(sqlCtx)
	for {
		row, err := rowIter.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		var vErr errhand.VerboseError
		unwrapString := func(val interface{}) (string, errhand.VerboseError) {
			s, ok, err := sql.Unwrap[string](sqlCtx, val)
			if err != nil {
				return "", errhand.VerboseErrorFromError(err)
			}
			if !ok {
				return "", errhand.BuildDError("expected string, got %T", val).Build()
			}
			return s, nil
		}
		var fragmentName string
		if row[0] != nil {
			fragmentName, vErr = unwrapString(row[0])
			if vErr != nil {
				return vErr
			}
		} else {
			fragmentName, vErr = unwrapString(row[1])
			if vErr != nil {
				return vErr
			}
		}

		var fragmentType string
		if row[2] != nil {
			fragmentType, vErr = unwrapString(row[2])
			if vErr != nil {
				return vErr
			}
		} else {
			fragmentType, vErr = unwrapString(row[3])
			if vErr != nil {
				return vErr
			}
		}

		var oldFragment string
		var newFragment string
		if row[4] != nil {
			oldFragment, vErr = unwrapString(row[4])
			if vErr != nil {
				return vErr
			}
			// Typically schema fragments have the semicolons stripped, so put it back on
			if len(oldFragment) > 0 && oldFragment[len(oldFragment)-1] != ';' {
				oldFragment += ";"
			}
		}
		if row[5] != nil {
			newFragment, vErr = unwrapString(row[5])
			if vErr != nil {
				return vErr
			}
			// Typically schema fragments have the semicolons stripped, so put it back on
			if len(newFragment) > 0 && newFragment[len(newFragment)-1] != ';' {
				newFragment += ";"
			}
		}

		switch fragmentType {
		case "event":
			err := dw.WriteEventDiff(sqlCtx, fragmentName, oldFragment, newFragment)
			if err != nil {
				return nil
			}
		case "trigger":
			err := dw.WriteTriggerDiff(sqlCtx, fragmentName, oldFragment, newFragment)
			if err != nil {
				return nil
			}
		case "view":
			err := dw.WriteViewDiff(sqlCtx, fragmentName, oldFragment, newFragment)
			if err != nil {
				return nil
			}
		default:
			cli.PrintErrf("Unrecognized schema element type: %s", fragmentType)
			continue
		}
	}

	return nil
}

func diffDatabase(
	queryist cli.Queryist,
	sqlCtx *sql.Context,
	tableSummary diff.TableDeltaSummary,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	if dArgs.diffParts&NameOnlyDiff != 0 {
		cli.Println(tableSummary.FromTableName)
		return nil
	}

	err := dw.BeginTable(tableSummary.FromTableName.Name, tableSummary.ToTableName.Name, tableSummary.IsAdd(), tableSummary.IsDrop())
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if dArgs.diffParts&SchemaOnlyDiff == 0 {
		return nil
	}

	fromTable := tableSummary.FromTableName
	var fromTableInfo *diff.TableInfo
	from, err := getDatabaseInfoAtRef(queryist, sqlCtx, fromTable.Name, dArgs.fromRef)
	if err == nil {
		// TODO: implement show create database as of ...
		fromTableInfo = &from
		fromTableInfo.CreateStmt = ""
	}

	toTable := tableSummary.ToTableName
	var toTableInfo *diff.TableInfo
	to, err := getDatabaseInfoAtRef(queryist, sqlCtx, toTable.Name, dArgs.toRef)
	if err == nil {
		toTableInfo = &to
	}

	err = dw.WriteTableSchemaDiff(fromTableInfo, toTableInfo, tableSummary)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	verr := diffRows(queryist, sqlCtx, tableSummary, fromTableInfo, toTableInfo, dArgs, dw)
	if verr != nil {
		return verr
	}

	return nil
}

// arePrimaryKeySetsDiffable checks if two schemas are diffable. Assumes the
// passed in schema are from the same table between commits.
func arePrimaryKeySetsDiffable(fromTableInfo, toTableInfo *diff.TableInfo) bool {
	var fromSch schema.Schema = nil
	var toSch schema.Schema = nil
	if fromTableInfo != nil {
		fromSch = fromTableInfo.Sch
	}
	if toTableInfo != nil {
		toSch = toTableInfo.Sch
	}

	if fromSch == nil && toSch == nil {
		return false
		// Empty case
	} else if fromSch == nil || fromSch.GetAllCols().Size() == 0 ||
		toSch == nil || toSch.GetAllCols().Size() == 0 {
		return true
	}

	// Keyless case for comparing
	if schema.IsKeyless(fromSch) && schema.IsKeyless(toSch) {
		return true
	}

	cc1 := fromSch.GetPKCols()
	cc2 := toSch.GetPKCols()

	if cc1.Size() != cc2.Size() {
		return false
	}

	for i := 0; i < cc1.Size(); i++ {
		c1 := cc1.GetByIndex(i)
		c2 := cc2.GetByIndex(i)
		if c1.IsPartOfPK != c2.IsPartOfPK {
			return false
		}
		if !c1.TypeInfo.ToSqlType().Equals(c2.TypeInfo.ToSqlType()) {
			return false
		}
	}

	return true
}

func diffRows(
	queryist cli.Queryist,
	sqlCtx *sql.Context,
	tableSummary diff.TableDeltaSummary,
	fromTableInfo, toTableInfo *diff.TableInfo,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	diffable := arePrimaryKeySetsDiffable(fromTableInfo, toTableInfo)
	canSqlDiff := !(toTableInfo == nil || (fromTableInfo != nil && !schema.SchemasAreEqual(fromTableInfo.Sch, toTableInfo.Sch)))

	var toSch, fromSch sql.Schema
	if fromTableInfo != nil {
		pkSch, err := sqlutil.FromDoltSchema(sqlCtx.GetCurrentDatabase(), fromTableInfo.Name, fromTableInfo.Sch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		fromSch = pkSch.Schema
	}
	if toTableInfo != nil {
		pkSch, err := sqlutil.FromDoltSchema(sqlCtx.GetCurrentDatabase(), toTableInfo.Name, toTableInfo.Sch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		toSch = pkSch.Schema
	}

	var unionSch sql.Schema
	if fromSch.Equals(toSch) {
		unionSch = fromSch
	} else {
		unionSch = unionSchemas(fromSch, toSch)
	}

	// We always instantiate a RowWriter in case the diffWriter needs it to close off any work from schema output
	rowWriter, err := dw.RowWriter(fromTableInfo, toTableInfo, tableSummary, unionSch)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// can't diff
	if !diffable {
		// TODO: this messes up some structured output if the user didn't redirect it
		cli.PrintErrf("Primary key sets differ between revisions for table '%s', skipping data diff\n", tableSummary.ToTableName)
		err = rowWriter.Close(sqlCtx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	} else if dArgs.diffOutput == SQLDiffOutput && !canSqlDiff {
		// TODO: this is overly broad, we can absolutely do better
		_, _ = fmt.Fprintf(cli.CliErr, "Incompatible schema change, skipping data diff for table '%s'\n", tableSummary.ToTableName)
		err = rowWriter.Close(sqlCtx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	// no data diff requested
	if dArgs.diffParts&DataOnlyDiff == 0 {
		err = rowWriter.Close(sqlCtx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	// do the data diff
	// TODO: schema names
	tableName := tableSummary.ToTableName.Name
	if len(tableName) == 0 {
		tableName = tableSummary.FromTableName.Name
	}

	if strings.HasPrefix(tableName, diff.DBPrefix) {
		err = rowWriter.Close(sqlCtx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	}

	columnNames, format := getColumnNames(fromTableInfo, toTableInfo)
	query := fmt.Sprintf("select %s ? from dolt_diff(?, ?, ?)", format)
	var params []interface{}
	for _, col := range columnNames {
		params = append(params, dbr.I(col))
	}
	params = append(params, dbr.I("diff_type"), dArgs.fromRef, dArgs.toRef, tableName)

	if len(dArgs.where) > 0 {
		query += " where ?"
		params = append(params, dbr.Expr(dArgs.where))
	}

	if dArgs.limit >= 0 {
		query += " limit ?"
		params = append(params, dbr.Expr(strconv.Itoa(dArgs.limit)))
	}

	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("Error building diff query:\n%s", interpolatedQuery).AddCause(err).Build()
	}

	sch, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if sql.ErrSyntaxError.Is(err) {
		return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", interpolatedQuery).AddCause(err).Build()
	} else if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", interpolatedQuery).AddCause(err).Build()
	}

	defer rowIter.Close(sqlCtx)
	defer rowWriter.Close(sqlCtx)

	var modifiedColNames map[string]bool
	if dArgs.skinny {
		modifiedColNames, err = getModifiedCols(sqlCtx, rowIter, unionSch, sch)
		if err != nil {
			return errhand.BuildDError("Error running diff query:\n%s", interpolatedQuery).AddCause(err).Build()
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
		rowWriter, err = dw.RowWriter(fromTableInfo, toTableInfo, tableSummary, filteredUnionSch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		defer rowWriter.Close(sqlCtx)

		// reset the row iterator
		err = rowIter.Close(sqlCtx)
		if err != nil {
			return errhand.BuildDError("Error closing row iterator:\n%s", interpolatedQuery).AddCause(err).Build()
		}
		_, rowIter, _, err = queryist.Query(sqlCtx, interpolatedQuery)
		defer rowIter.Close(sqlCtx)
		if sql.ErrSyntaxError.Is(err) {
			return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", interpolatedQuery).AddCause(err).Build()
		} else if err != nil {
			return errhand.BuildDError("Error running diff query:\n%s", interpolatedQuery).AddCause(err).Build()
		}
	}

	err = writeDiffResults(sqlCtx, sch, unionSch, rowIter, rowWriter, modifiedColNames, dArgs)
	if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", interpolatedQuery).AddCause(err).Build()
	}

	return nil
}

func unionSchemas(s1 sql.Schema, s2 sql.Schema) sql.Schema {
	var union sql.Schema
	union = append(union, s1...)
	for i := range s2 {
		idx := union.IndexOfColName(s2[i].Name)
		if idx < 0 {
			union = append(union, s2[i])
		} else {
			// have same name. choose the one with the most 'flexible' type
			orig := union[idx]
			orig.Type = chooseMostFlexibleType(orig.Type, s2[i].Type)
		}
	}
	return union
}

// chooseMostFlexibleType returns the type that is more 'flexible' than the other type, for the purposes of printing
// a diff when the type of the column has changed. There are a ton of different ways we could slice this. We'll stick to
// the following rules for the time being:
//   - Going from any integer to a float, always take the float.
//   - Going from any integer to a decimal, always take the decimal.
//   - Going from any decimal to a decimal (with different precision/scale), always take the new decimal.
//   - Going from a low precision float to a high precision float, we'll always take the high precision float.
//   - Going from a low precision integer to a high precision integer, we'll always take the high precision integer.
//     Currently, we only support this if the signage is the same.
//   - Going from a DATE, TIME, or DATETIME to a TIMESTAMP, we'll always take the TIMESTAMP.
//
// If none of these rules apply, we'll just take the `a` type.
//
// Note this is only for printing the diff. This is not robust for other purposes.
func chooseMostFlexibleType(origA, origB sql.Type) sql.Type {
	if origA.Equals(origB) {
		return origA
	}

	at := origA.Type()
	bt := origB.Type()

	// If both are numbers, we'll take the float.
	if sqltypes.IsIntegral(at) && sqltypes.IsFloat(bt) {
		return origB
	}
	if sqltypes.IsIntegral(bt) && sqltypes.IsFloat(at) {
		return origA
	}

	if bt == sqltypes.Decimal && sqltypes.IsIntegral(at) {
		return origB
	}

	if sqltypes.IsFloat(at) && sqltypes.IsFloat(bt) {
		// There are only two float types, so we'll always end up with a float64 here.
		return origA.Promote()
	}

	if sqltypes.IsIntegral(at) && sqltypes.IsIntegral(bt) {
		if (sqltypes.IsUnsigned(at) && sqltypes.IsUnsigned(bt)) || (!sqltypes.IsUnsigned(at) && !sqltypes.IsUnsigned(bt)) {
			// Vitess definitions are ordered in the even that both are signed or unsigned, so take the higher one.
			if bt > at {
				return origB
			}
			return origA
		}

		// TODO: moving from unsigned to signed or vice versa.
	}

	if bt == sqltypes.Decimal && at == sqltypes.Decimal {
		return origB
	}

	if bt == sqltypes.Timestamp && (at == sqltypes.Date || at == sqltypes.Time || at == sqltypes.Datetime) {
		return origB
	}

	return origA
}

func getColumnNames(fromTableInfo, toTableInfo *diff.TableInfo) (colNames []string, formatText string) {
	var fromSch, toSch schema.Schema
	if fromTableInfo != nil {
		fromSch = fromTableInfo.Sch
	}
	if toTableInfo != nil {
		toSch = toTableInfo.Sch
	}

	var cols []string
	if fromSch != nil {
		fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("from_%s", col.Name))
			return false, nil
		})
	}
	if toSch != nil {
		toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("to_%s", col.Name))
			return false, nil
		})
	}

	colNames = cols
	formatText = strings.Repeat("?, ", len(cols))
	return colNames, formatText
}

func writeDiffResults(
	ctx *sql.Context,
	diffQuerySch sql.Schema,
	targetSch sql.Schema,
	iter sql.RowIter,
	writer diff.SqlRowDiffWriter,
	modifiedColNames map[string]bool,
	dArgs *diffArgs,
) error {
	ds, err := diff.NewDiffSplitter(diffQuerySch, targetSch)
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

		oldRow, newRow, err := ds.SplitDiffResultRow(r)
		if err != nil {
			return err
		}

		if dArgs.skinny {
			var filteredOldRow, filteredNewRow diff.RowDiff
			for i, changeType := range newRow.ColDiffs {
				if (changeType == diff.Added|diff.Removed) || modifiedColNames[targetSch[i].Name] {
					if i < len(oldRow.Row) {
						filteredOldRow.Row = append(filteredOldRow.Row, oldRow.Row[i])
						filteredOldRow.ColDiffs = append(filteredOldRow.ColDiffs, oldRow.ColDiffs[i])
						filteredOldRow.RowDiff = oldRow.RowDiff
					}

					if i < len(newRow.Row) {
						filteredNewRow.Row = append(filteredNewRow.Row, newRow.Row[i])
						filteredNewRow.ColDiffs = append(filteredNewRow.ColDiffs, newRow.ColDiffs[i])
						filteredNewRow.RowDiff = newRow.RowDiff
					}
				}
			}

			oldRow = filteredOldRow
			newRow = filteredNewRow
		}

		// We are guaranteed to have "ModeRow" for writers that do not support combined rows
		if dArgs.diffMode != diff.ModeRow && oldRow.RowDiff == diff.ModifiedOld && newRow.RowDiff == diff.ModifiedNew {
			if err = writer.WriteCombinedRow(ctx, oldRow.Row, newRow.Row, dArgs.diffMode); err != nil {
				return err
			}
		} else {
			if oldRow.Row != nil {
				if err = writer.WriteRow(ctx, oldRow.Row, oldRow.RowDiff, oldRow.ColDiffs); err != nil {
					return err
				}
			}
			if newRow.Row != nil {
				if err = writer.WriteRow(ctx, newRow.Row, newRow.RowDiff, newRow.ColDiffs); err != nil {
					return err
				}
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

		ds, err := diff.NewDiffSplitter(diffQuerySch, unionSch)
		if err != nil {
			return modifiedColNames, err
		}

		oldRow, newRow, err := ds.SplitDiffResultRow(r)
		if err != nil {
			return modifiedColNames, err
		}

		for i, changeType := range newRow.ColDiffs {
			if changeType != diff.None || unionSch[i].PrimaryKey {
				modifiedColNames[unionSch[i].Name] = true
			}
		}

		for i, changeType := range oldRow.ColDiffs {
			if changeType != diff.None || unionSch[i].PrimaryKey {
				modifiedColNames[unionSch[i].Name] = true
			}
		}
	}

	return modifiedColNames, nil
}
