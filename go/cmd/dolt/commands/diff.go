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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

type diffOutput int
type diffPart int

const (
	SchemaOnlyDiff diffPart = 1 // 0b0001
	DataOnlyDiff   diffPart = 2 // 0b0010
	Stat           diffPart = 4 // 0b0100
	Summary        diffPart = 8 // 0b1000

	SchemaAndDataDiff = SchemaOnlyDiff | DataOnlyDiff

	TabularDiffOutput diffOutput = 1
	SQLDiffOutput     diffOutput = 2
	JsonDiffOutput    diffOutput = 3

	DataFlag    = "data"
	SchemaFlag  = "schema"
	StatFlag    = "stat"
	SummaryFlag = "summary"
	whereParam  = "where"
	limitParam  = "limit"
	SkinnyFlag  = "skinny"
	MergeBase   = "merge-base"
	DiffMode    = "diff-mode"
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

type diffArgs struct {
	diffParts  diffPart
	diffOutput diffOutput
	diffMode   diff.Mode
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
	ap.SupportsFlag(StatFlag, "", "Show stats of data changes")
	ap.SupportsFlag(SummaryFlag, "", "Show summary of data and schema changes")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format diff output. Valid values are tabular, sql, json. Defaults to tabular.")
	ap.SupportsString(whereParam, "", "column", "filters columns based on values in the diff.  See {{.EmphasisLeft}}dolt diff --help{{.EmphasisRight}} for details.")
	ap.SupportsInt(limitParam, "", "record_count", "limits to the first N diffs.")
	ap.SupportsFlag(cli.CachedFlag, "c", "Show only the staged data changes.")
	ap.SupportsFlag(SkinnyFlag, "sk", "Shows only primary key columns and any columns with data changes.")
	ap.SupportsFlag(MergeBase, "", "Uses merge base of the first commit and second commit (or HEAD if not supplied) as the first commit")
	ap.SupportsString(DiffMode, "", "diff mode", "Determines how to display modified rows with tabular output. Valid values are row, line, in-place, context. Defaults to context.")
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
	if apr.Contains(StatFlag) || apr.Contains(SummaryFlag) {
		if apr.Contains(SchemaFlag) || apr.Contains(DataFlag) {
			return errhand.BuildDError("invalid Arguments: --stat and --summary cannot be combined with --schema or --data").Build()
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
	} else if apr.Contains(StatFlag) {
		dArgs.diffParts = Stat
	} else if apr.Contains(SummaryFlag) {
		dArgs.diffParts = Summary
	}

	dArgs.skinny = apr.Contains(SkinnyFlag)

	f := apr.GetValueOrDefault(FormatFlag, "tabular")
	switch strings.ToLower(f) {
	case "tabular":
		dArgs.diffOutput = TabularDiffOutput
		switch strings.ToLower(apr.GetValueOrDefault(DiffMode, "context")) {
		case "row":
			dArgs.diffMode = diff.ModeRow
		case "line":
			dArgs.diffMode = diff.ModeLine
		case "in-place":
			dArgs.diffMode = diff.ModeInPlace
		case "context":
			dArgs.diffMode = diff.ModeContext
		}
	case "sql":
		dArgs.diffOutput = SQLDiffOutput
	case "json":
		dArgs.diffOutput = JsonDiffOutput
	}

	dArgs.limit, _ = apr.GetInt(limitParam)
	dArgs.where = apr.GetValueOrDefault(whereParam, "")

	tableNames, err := dArgs.applyDiffRoots(ctx, dEnv, apr.Args, apr.Contains(cli.CachedFlag), apr.Contains(MergeBase))
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
		if ok {
			dArgs.tableSet.Add(tableName)
			continue
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
func (dArgs *diffArgs) applyDiffRoots(ctx context.Context, dEnv *env.DoltEnv, args []string, isCached, useMergeBase bool) ([]string, error) {
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
	dArgs.fromRef = doltdb.Staged
	dArgs.toRoot = workingRoot
	dArgs.toRef = doltdb.Working
	if isCached {
		dArgs.fromRoot = headRoot
		dArgs.fromRef = "HEAD"
		dArgs.toRoot = stagedRoot
		dArgs.toRef = doltdb.Staged
	}

	if len(args) == 0 {
		if useMergeBase {
			return nil, fmt.Errorf("Must supply at least one revision when using --merge-base flag")
		}
		// `dolt diff`
		return nil, nil
	}

	if strings.Contains(args[0], "..") {
		if useMergeBase {
			return nil, fmt.Errorf("Cannot use `..` or `...` with --merge-base flag")
		}
		err = dArgs.applyDotRevisions(ctx, dEnv, args)
		if err != nil {
			return nil, err
		}
		return args[1:], err
	}

	// treat the first arg as a ref spec
	fromRoot, ok := diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, args[0])
	// if it doesn't resolve, treat it as a table name
	if !ok {
		// `dolt diff table`
		if useMergeBase {
			return nil, fmt.Errorf("Must supply at least one revision when using --merge-base flag")
		}
		return args, nil
	}

	dArgs.fromRoot = fromRoot
	dArgs.fromRef = args[0]

	if len(args) == 1 {
		// `dolt diff from_commit`
		if useMergeBase {
			err := dArgs.applyMergeBase(ctx, dEnv, args[0], "HEAD")
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	toRoot, ok := diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, args[1])
	if !ok {
		// `dolt diff from_commit [...tables]`
		if useMergeBase {
			err := dArgs.applyMergeBase(ctx, dEnv, args[0], "HEAD")
			if err != nil {
				return nil, err
			}
		}
		return args[1:], nil
	}

	dArgs.toRoot = toRoot
	dArgs.toRef = args[1]

	if useMergeBase {
		err := dArgs.applyMergeBase(ctx, dEnv, args[0], args[1])
		if err != nil {
			return nil, err
		}
	}

	// `dolt diff from_commit to_commit [...tables]`
	return args[2:], nil
}

// applyMergeBase applies the merge base of two revisions to the |from| root
// values.
func (dArgs *diffArgs) applyMergeBase(ctx context.Context, dEnv *env.DoltEnv, leftStr, rightStr string) error {
	mergeBaseStr, err := getMergeBaseFromStrings(ctx, dEnv, leftStr, rightStr)
	if err != nil {
		return err
	}

	fromRoot, ok := diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, mergeBaseStr)
	if !ok {
		return fmt.Errorf("merge base invalid %s", mergeBaseStr)
	}

	dArgs.fromRoot = fromRoot
	dArgs.fromRef = mergeBaseStr

	return nil
}

// applyDotRevisions applies the appropriate |from| and |to| root values to the
// receiver for arguments containing `..` or `...`
func (dArgs *diffArgs) applyDotRevisions(ctx context.Context, dEnv *env.DoltEnv, args []string) error {
	// `dolt diff from_commit...to_commit [...tables]`
	if strings.Contains(args[0], "...") {
		refs := strings.Split(args[0], "...")
		var toRoot *doltdb.RootValue
		ok := true

		if len(refs[0]) > 0 {
			right := refs[1]
			// Use current HEAD if right side of `...` does not exist
			if len(refs[1]) == 0 {
				right = "HEAD"
			}

			err := dArgs.applyMergeBase(ctx, dEnv, refs[0], right)
			if err != nil {
				return err
			}
		}

		if len(refs[1]) > 0 {
			if toRoot, ok = diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, refs[1]); !ok {
				return fmt.Errorf("to ref in three dot diff must be valid ref: %s", refs[1])
			}
			dArgs.toRoot = toRoot
			dArgs.toRef = refs[1]
		}

		return nil
	}

	// `dolt diff from_commit..to_commit [...tables]`
	if strings.Contains(args[0], "..") {
		refs := strings.Split(args[0], "..")
		var fromRoot *doltdb.RootValue
		var toRoot *doltdb.RootValue
		ok := true

		if len(refs[0]) > 0 {
			if fromRoot, ok = diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, refs[0]); !ok {
				return fmt.Errorf("from ref in two dot diff must be valid ref: %s", refs[0])
			}
			dArgs.fromRoot = fromRoot
			dArgs.fromRef = refs[0]
		}

		if len(refs[1]) > 0 {
			if toRoot, ok = diff.MaybeResolveRoot(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, refs[1]); !ok {
				return fmt.Errorf("to ref in two dot diff must be valid ref: %s", refs[1])
			}
			dArgs.toRoot = toRoot
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

func printDiffSummary(ctx context.Context, tds []diff.TableDelta, dArgs *diffArgs) errhand.VerboseError {
	cliWR := iohelp.NopWrCloser(cli.OutStream)
	wr := tabular.NewFixedWidthTableWriter(diffSummarySchema, cliWR, 100)
	defer wr.Close(ctx)

	for _, td := range tds {
		if !dArgs.tableSet.Contains(td.FromName) && !dArgs.tableSet.Contains(td.ToName) {
			continue
		}

		if td.FromTable == nil && td.ToTable == nil {
			return errhand.BuildDError("error: both tables in tableDelta are nil").Build()
		}

		summ, err := td.GetSummary(ctx)
		if err != nil {
			return errhand.BuildDError("could not get table delta summary").AddCause(err).Build()
		}
		tableName := summ.TableName
		if summ.DiffType == "renamed" {
			tableName = fmt.Sprintf("%s -> %s", summ.FromTableName, summ.ToTableName)
		}

		err = wr.WriteSqlRow(ctx, sql.Row{tableName, summ.DiffType, summ.DataChange, summ.SchemaChange})
		if err != nil {
			return errhand.BuildDError("could not write table delta summary").AddCause(err).Build()
		}
	}

	return nil
}

func diffUserTables(ctx context.Context, dEnv *env.DoltEnv, dArgs *diffArgs) errhand.VerboseError {
	var err error

	tableDeltas, err := diff.GetTableDeltas(ctx, dArgs.fromRoot, dArgs.toRoot)
	if err != nil {
		return errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
	}

	sqlEng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sqlCtx, err := sqlEng.NewLocalContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	sqlCtx.SetCurrentDatabase(dbName)

	sort.Slice(tableDeltas, func(i, j int) bool {
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})

	if dArgs.diffParts&Summary != 0 {
		return printDiffSummary(ctx, tableDeltas, dArgs)
	}

	dw, err := newDiffWriter(dArgs.diffOutput)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	doltSchemasChanged := false
	for _, td := range tableDeltas {
		if !shouldPrintTableDelta(dArgs.tableSet, td) {
			continue
		}

		if isDoltSchemasTable(td) {
			// save dolt_schemas table diff for last in diff output
			doltSchemasChanged = true
		} else {
			verr := diffUserTable(sqlCtx, td, sqlEng, dArgs, dw)
			if verr != nil {
				return verr
			}
		}
	}

	if doltSchemasChanged {
		verr := diffDoltSchemasTable(sqlCtx, sqlEng, dArgs, dw)
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

func shouldPrintTableDelta(tablesToPrint *set.StrSet, td diff.TableDelta) bool {
	// TODO: this should be case insensitive
	return tablesToPrint.Contains(td.FromName) || tablesToPrint.Contains(td.ToName)
}

func isDoltSchemasTable(td diff.TableDelta) bool {
	return td.FromName == doltdb.SchemasTableName || td.ToName == doltdb.SchemasTableName
}

func diffUserTable(
	ctx *sql.Context,
	td diff.TableDelta,
	sqlEng *engine.SqlEngine,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
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

	if dArgs.diffParts&Stat != 0 {
		return printDiffStat(ctx, td, fromSch.GetAllCols().Size(), toSch.GetAllCols().Size())
	}

	if dArgs.diffParts&SchemaOnlyDiff != 0 {
		err := dw.WriteTableSchemaDiff(ctx, dArgs.toRoot, td)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	if td.IsDrop() && dArgs.diffOutput == SQLDiffOutput {
		return nil // don't output DELETE FROM statements after DROP TABLE
	} else if td.IsAdd() {
		fromSch = toSch
	}

	verr := diffRows(ctx, sqlEng, td, dArgs, dw)
	if verr != nil {
		return verr
	}

	return nil
}

func diffDoltSchemasTable(
	sqlCtx *sql.Context,
	sqlEng *engine.SqlEngine,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
	query := fmt.Sprintf("select from_name,to_name,from_type,to_type,from_fragment,to_fragment "+
		"from dolt_diff('%s','%s','%s') "+
		"order by coalesce(from_type, to_type), coalesce(from_name, to_name)",
		dArgs.fromRef, dArgs.toRef, doltdb.SchemasTableName)

	_, rowIter, err := sqlEng.Query(sqlCtx, query)
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

		var fragmentName string
		if row[0] != nil {
			fragmentName = row[0].(string)
		} else {
			fragmentName = row[1].(string)
		}

		var fragmentType string
		if row[2] != nil {
			fragmentType = row[2].(string)
		} else {
			fragmentType = row[3].(string)
		}

		var oldFragment string
		var newFragment string
		if row[4] != nil {
			oldFragment = row[4].(string)
			// Typically schema fragements have the semicolons stripped, so put it back on
			if len(oldFragment) > 0 && oldFragment[len(oldFragment)-1] != ';' {
				oldFragment += ";"
			}
		}
		if row[5] != nil {
			newFragment = row[5].(string)
			// Typically schema fragements have the semicolons stripped, so put it back on
			if len(newFragment) > 0 && newFragment[len(newFragment)-1] != ';' {
				newFragment += ";"
			}
		}

		switch fragmentType {
		case "view":
			err := dw.WriteViewDiff(sqlCtx, fragmentName, oldFragment, newFragment)
			if err != nil {
				return nil
			}
		case "trigger":
			err := dw.WriteTriggerDiff(sqlCtx, fragmentName, oldFragment, newFragment)
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

func diffRows(
	ctx *sql.Context,
	sqlEng *engine.SqlEngine,
	td diff.TableDelta,
	dArgs *diffArgs,
	dw diffWriter,
) errhand.VerboseError {
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
		cli.PrintErrf("Primary key sets differ between revisions for table '%s', skipping data diff\n", td.ToName)
		err := rowWriter.Close(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		return nil
	} else if dArgs.diffOutput == SQLDiffOutput && !canSqlDiff {
		// TODO: this is overly broad, we can absolutely do better
		_, _ = fmt.Fprintf(cli.CliErr, "Incompatible schema change, skipping data diff for table '%s'\n", td.ToName)
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
	query := fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columns, "diff_type", dArgs.fromRef, dArgs.toRef, tableName)

	if len(dArgs.where) > 0 {
		query += " where " + dArgs.where
	}

	if dArgs.limit >= 0 {
		query += " limit " + strconv.Itoa(dArgs.limit)
	}

	sch, rowIter, err := sqlEng.Query(ctx, query)
	if sql.ErrSyntaxError.Is(err) {
		return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", query).AddCause(err).Build()
	} else if err != nil {
		return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
	}

	defer rowIter.Close(ctx)
	defer rowWriter.Close(ctx)

	var modifiedColNames map[string]bool
	if dArgs.skinny {
		modifiedColNames, err = getModifiedCols(ctx, rowIter, unionSch, sch)
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
		err = rowIter.Close(ctx)
		if err != nil {
			return errhand.BuildDError("Error closing row iterator:\n%s", query).AddCause(err).Build()
		}
		_, rowIter, err = sqlEng.Query(ctx, query)
		defer rowIter.Close(ctx)
		if sql.ErrSyntaxError.Is(err) {
			return errhand.BuildDError("Failed to parse diff query. Invalid where clause?\nDiff query: %s", query).AddCause(err).Build()
		} else if err != nil {
			return errhand.BuildDError("Error running diff query:\n%s", query).AddCause(err).Build()
		}
	}

	err = writeDiffResults(ctx, sch, unionSch, rowIter, rowWriter, modifiedColNames, dArgs)
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
