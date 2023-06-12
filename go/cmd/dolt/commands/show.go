// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var hashRegex = regexp.MustCompile(`^#?[0-9a-v]{32}$`)

type showDatasets struct {
	fromRoot *doltdb.RootValue
	toRoot   *doltdb.RootValue
	fromRef  string
	toRef    string
}

type showArgs struct {
	*diffDisplaySettings
	*showDatasets
	tableSet *set.StrSet
}

type showOpts struct {
	showParents bool
	pretty      bool
	decoration  string
	specRefs    []string

	*diffDisplaySettings
}

var showDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show information about a specific commit`,
	LongDesc:  `Show information about a specific commit`,
	Synopsis: []string{
		`[{{.LessThan}}revision{{.GreaterThan}}]`,
	},
}

type ShowCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ShowCmd) Name() string {
	return "show"
}

// Description returns a description of the command
func (cmd ShowCmd) Description() string {
	return "Show information about a specific commit."
}

// EventType returns the type of the event to log
func (cmd ShowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SHOW
}

func (cmd ShowCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(showDocs, ap)
}

func (cmd ShowCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	// Flags inherited from Log
	ap.SupportsFlag(cli.ParentsFlag, "", "Shows all parents of each commit in the log.")
	ap.SupportsString(cli.DecorateFlag, "", "decorate_fmt", "Shows refs next to commits. Valid options are short, full, no, and auto")
	ap.SupportsFlag(cli.NoPrettyFlag, "", "Show the object without making it pretty.")

	// Flags inherited from Diff
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
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, showDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	opts, err := parseShowArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if err := cmd.validateArgs(apr); err != nil {
		return handleErrAndExit(err)
	}

	if !opts.pretty && !dEnv.DoltDB.Format().UsesFlatbuffers() {
		cli.PrintErrln("dolt show --no-pretty is not supported when using old LD_1 storage format.")
		return 1
	}

	opts.diffDisplaySettings = parseDiffDisplaySettings(apr)

	err = showObjects(ctx, dEnv, opts)

	return handleErrAndExit(err)
}

func (cmd ShowCmd) validateArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
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

func parseShowArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*showOpts, error) {

	decorateOption := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return nil, fmt.Errorf("fatal: invalid --decorate option: %s", decorateOption)
	}

	return &showOpts{
		showParents: apr.Contains(cli.ParentsFlag),
		pretty:      !apr.Contains(cli.NoPrettyFlag),
		decoration:  decorateOption,
		specRefs:    apr.Args,
	}, nil
}

func showObjects(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts) error {
	if len(opts.specRefs) == 0 {
		headRef, err := dEnv.RepoStateReader().CWBHeadSpec()
		if err != nil {
			return err
		}
		return showCommitSpec(ctx, dEnv, opts, headRef)
	}

	for _, specRef := range opts.specRefs {
		err := showSpecRef(ctx, dEnv, opts, specRef)
		if err != nil {
			return err
		}
	}

	return nil
}

// parseHashString converts a string representing a hash into a hash.Hash.
func parseHashString(hashStr string) (hash.Hash, error) {
	unprefixed := strings.TrimPrefix(hashStr, "#")
	parsedHash, ok := hash.MaybeParse(unprefixed)
	if !ok {
		return hash.Hash{}, errors.New("invalid hash: " + hashStr)
	}
	return parsedHash, nil
}

func showSpecRef(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts, specRef string) error {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return err
	}

	upperCaseSpecRef := strings.ToUpper(specRef)
	if upperCaseSpecRef == doltdb.Working || upperCaseSpecRef == doltdb.Staged || hashRegex.MatchString(specRef) {
		var refHash hash.Hash
		var err error
		if upperCaseSpecRef == doltdb.Working {
			refHash, err = roots.Working.HashOf()
		} else if upperCaseSpecRef == doltdb.Staged {
			refHash, err = roots.Staged.HashOf()
		} else {
			refHash, err = parseHashString(specRef)
		}
		if err != nil {
			return err
		}
		value, err := dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, refHash)
		if err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("Unable to resolve object ref %s", specRef)
		}

		if !opts.pretty {
			cli.Println(value.Kind(), value.HumanReadableString())
		}

		// If this is a commit, use the pretty printer. To determine whether it's a commit, try calling NewCommitFromValue.
		commit, err := doltdb.NewCommitFromValue(ctx, dEnv.DoltDB.ValueReadWriter(), dEnv.DoltDB.NodeStore(), value)

		if err == datas.ErrNotACommit {
			if !dEnv.DoltDB.Format().UsesFlatbuffers() {
				return fmt.Errorf("dolt show cannot show non-commit objects when using the old LD_1 storage format: %s is not a commit", specRef)
			}
			cli.Println(value.Kind(), value.HumanReadableString())
		} else if err == nil {
			showCommit(ctx, dEnv, opts, commit)
		} else {
			return err
		}
	} else { // specRef is a CommitSpec, which must resolve to a Commit.
		commitSpec, err := getCommitSpec(specRef)
		if err != nil {
			return err
		}

		err = showCommitSpec(ctx, dEnv, opts, commitSpec)
		if err != nil {
			return err
		}
	}
	return nil
}

func showCommitSpec(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts, commitSpec *doltdb.CommitSpec) error {

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return err
	}

	commit, err := dEnv.DoltDB.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return err
	}

	if opts.pretty {
		err = showCommit(ctx, dEnv, opts, commit)
		if err != nil {
			return err
		}
	} else {
		value := commit.Value()
		cli.Println(value.Kind(), value.HumanReadableString())
	}
	return nil
}

func showCommit(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts, comm *doltdb.Commit) error {

	cHashToRefs, err := getHashToRefs(ctx, dEnv, opts.decoration)
	if err != nil {
		return err
	}

	meta, mErr := comm.GetCommitMeta(ctx)
	if mErr != nil {
		cli.PrintErrln("error: failed to get commit metadata")
		return err
	}
	pHashes, pErr := comm.ParentHashes(ctx)
	if pErr != nil {
		cli.PrintErrln("error: failed to get parent hashes")
		return err
	}
	cmHash, cErr := comm.HashOf()
	if cErr != nil {
		cli.PrintErrln("error: failed to get commit hash")
		return err
	}

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return err
	}
	cwbHash, err := dEnv.DoltDB.GetHashForRefStr(ctx, headRef.String())
	if err != nil {
		return err
	}

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommit(pager, 0, opts.showParents, opts.decoration, logNode{
			commitMeta:   meta,
			commitHash:   cmHash,
			parentHashes: pHashes,
			branchNames:  cHashToRefs[cmHash],
			isHead:       cmHash == *cwbHash})
	})

	if comm.NumParents() == 0 {
		return nil
	}

	if comm.NumParents() > 1 {
		return fmt.Errorf("requested commit is a merge commit. 'dolt show' currently only supports viewing non-merge commits")
	}

	commitRoot, err := comm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	parent, err := comm.GetParent(ctx, 0)
	if err != nil {
		return err
	}

	parentRoot, err := parent.GetRootValue(ctx)
	if err != nil {
		return err
	}

	parentHash, err := parent.HashOf()
	if err != nil {
		return err
	}

	datasets := &showDatasets{
		fromRoot: parentRoot,
		toRoot:   commitRoot,
		fromRef:  parentHash.String(),
		toRef:    cmHash.String(),
	}

	// An empty string will cause all tables to be printed.
	var tableNames []string

	tableSet, err := parseDiffTableSet(ctx, dEnv, datasets, tableNames)
	if err != nil {
		return err
	}

	dArgs := &showArgs{
		diffDisplaySettings: opts.diffDisplaySettings,
		showDatasets:        datasets,
		tableSet:            tableSet,
	}

	return diffUserTables(ctx, dEnv, dArgs)
}

func diffUserTables(ctx context.Context, dEnv *env.DoltEnv, dArgs *showArgs) errhand.VerboseError {
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

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(fmt.Errorf("couldn't get working root, cause: %w", err))
	}

	ignoredTablePatterns, err := doltdb.GetIgnoredTablePatterns(ctx, roots)
	if err != nil {
		return errhand.VerboseErrorFromError(fmt.Errorf("couldn't get ignored table patterns, cause: %w", err))
	}

	toRootHash, err := dArgs.showDatasets.toRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	fromRootHash, err := dArgs.showDatasets.fromRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	workingSetHash, err := roots.Working.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	cli.Printf("toRootHash = %s, fromRootHash = %s, workingSetHash = %s\n", toRootHash.String(), fromRootHash.String(), workingSetHash.String())

	doltSchemasChanged := false
	for _, td := range tableDeltas {
		// Don't print tables if one side of the diff is an ignored table in the working set being added.
		if toRootHash == workingSetHash && td.FromTable == nil {
			ignoreResult, err := ignoredTablePatterns.IsTableNameIgnored(td.ToName)
			if err != nil {
				return errhand.VerboseErrorFromError(err)
			}
			if ignoreResult == doltdb.Ignore {
				continue
			}
		}

		if fromRootHash == workingSetHash && td.ToTable == nil {
			ignoreResult, err := ignoredTablePatterns.IsTableNameIgnored(td.FromName)
			if err != nil {
				return errhand.VerboseErrorFromError(err)
			}
			if ignoreResult == doltdb.Ignore {
				continue
			}
		}

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

func diffUserTable(
	ctx *sql.Context,
	td diff.TableDeltaEngine,
	sqlEng *engine.SqlEngine,
	dArgs *showArgs,
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
		err := dw.WriteTableSchemaDiff(ctx, td)
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

func diffRows(
	ctx *sql.Context,
	sqlEng *engine.SqlEngine,
	td diff.TableDeltaEngine,
	dArgs *showArgs,
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

func diffDoltSchemasTable(
	sqlCtx *sql.Context,
	sqlEng *engine.SqlEngine,
	dArgs *showArgs,
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

func printDiffSummary(ctx context.Context, tds []diff.TableDeltaEngine, dArgs *showArgs) errhand.VerboseError {
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

func parseDiffTableSet(ctx context.Context, dEnv *env.DoltEnv, datasets *showDatasets, tableNames []string) (*set.StrSet, error) {

	tableSet := set.NewStrSet(nil)

	for _, tableName := range tableNames {
		// verify table args exist in at least one root
		_, ok, err := datasets.fromRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if ok {
			tableSet.Add(tableName)
			continue
		}

		_, ok, err = datasets.toRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if ok {
			tableSet.Add(tableName)
			continue
		}
		if !ok {
			return nil, fmt.Errorf("table %s does not exist in either revision", tableName)
		}
	}

	// if no tables or docs were specified as args, diff all tables and docs
	if len(tableNames) == 0 {
		utn, err := doltdb.UnionTableNames(ctx, datasets.fromRoot, datasets.toRoot)
		if err != nil {
			return nil, err
		}
		tableSet.Add(utn...)
	}

	return tableSet, nil
}

func writeDiffResults(
	ctx *sql.Context,
	diffQuerySch sql.Schema,
	targetSch sql.Schema,
	iter sql.RowIter,
	writer diff.SqlRowDiffWriter,
	modifiedColNames map[string]bool,
	dArgs *showArgs,
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
