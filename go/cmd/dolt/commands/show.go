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
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
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

type showOpts struct {
	showParents            bool
	pretty                 bool
	decoration             string
	specRefs               []string
	resolvedNonCommitSpecs map[string]string

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

func (cmd ShowCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, showDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	opts, err := parseShowArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if err := cmd.validateArgs(apr); err != nil {
		return handleErrAndExit(err)
	}

	opts.diffDisplaySettings = parseDiffDisplaySettings(apr)

	// Decide if we're going to use dolt or sql for this execution.
	// We can use SQL in the following cases:
	// 1. `--no-pretty` is not set, so we will be producing "pretty" output.
	// 2. opts.specRefs contains values that are NOT commit hashes.
	// In all other cases, we should use DoltEnv
	allSpecRefsAreCommits := true
	allSpecRefsAreNonCommits := true
	resolvedNonCommitSpecs := map[string]string{}
	for _, specRef := range opts.specRefs {
		isNonCommitSpec, resolvedValue, err := resolveNonCommitSpec(ctx, dEnv, specRef)
		if err != nil {
			err = fmt.Errorf("error resolving spec ref '%s': %w", specRef, err)
			return handleErrAndExit(err)
		}
		allSpecRefsAreNonCommits = allSpecRefsAreNonCommits && isNonCommitSpec
		allSpecRefsAreCommits = allSpecRefsAreCommits && !isNonCommitSpec

		if isNonCommitSpec {
			resolvedNonCommitSpecs[specRef] = resolvedValue
		}
	}

	if !allSpecRefsAreCommits && !allSpecRefsAreNonCommits {
		err = fmt.Errorf("cannot mix commit hashes and non-commit spec refs")
		return handleErrAndExit(err)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	useDoltEnv := !opts.pretty || (len(opts.specRefs) > 0 && allSpecRefsAreNonCommits)
	if useDoltEnv {
		_, ok := queryist.(*engine.SqlEngine)
		if !ok {
			cli.PrintErrln("`dolt show --no-pretty` or `dolt show NON_COMMIT_REF` only supported in local mode.")
			return 1
		}

		if !opts.pretty && !dEnv.DoltDB.Format().UsesFlatbuffers() {
			cli.PrintErrln("dolt show --no-pretty is not supported when using old LD_1 storage format.")
			return 1
		}
		opts.resolvedNonCommitSpecs = resolvedNonCommitSpecs
		err = printObjects(ctx, dEnv, opts)
		return handleErrAndExit(err)
	} else {
		err = printObjectsPretty(queryist, sqlCtx, opts)
		return handleErrAndExit(err)
	}
}

// resolveNonCommitSpec resolves a non-commit spec ref.
// A non-commit spec ref in this context is a ref that is returned by `dolt show --no-pretty` but is NOT a commit hash.
// These refs need env.DoltEnv in order to be resolved to a human-readable value.
func resolveNonCommitSpec(ctx context.Context, dEnv *env.DoltEnv, specRef string) (isNonCommitSpec bool, resolvedValue string, err error) {
	isNonCommitSpec = false
	resolvedValue = ""

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return isNonCommitSpec, resolvedValue, err
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
			return isNonCommitSpec, resolvedValue, err
		}
		value, err := dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, refHash)
		if err != nil {
			return isNonCommitSpec, resolvedValue, err
		}
		if value == nil {
			return isNonCommitSpec, resolvedValue, fmt.Errorf("Unable to resolve object ref %s", specRef)
		}

		// If this is a commit, use the pretty printer. To determine whether it's a commit, try calling NewCommitFromValue.
		_, err = doltdb.NewCommitFromValue(ctx, dEnv.DoltDB.ValueReadWriter(), dEnv.DoltDB.NodeStore(), value)

		if err == datas.ErrNotACommit {
			if !dEnv.DoltDB.Format().UsesFlatbuffers() {
				return isNonCommitSpec, resolvedValue, fmt.Errorf("dolt show cannot show non-commit objects when using the old LD_1 storage format: %s is not a commit", specRef)
			}
			isNonCommitSpec = true
			resolvedValue = fmt.Sprintln(value.Kind(), value.HumanReadableString())
			return isNonCommitSpec, resolvedValue, nil
		} else if err == nil {
			isNonCommitSpec = false
			return isNonCommitSpec, resolvedValue, nil
		} else {
			return isNonCommitSpec, resolvedValue, err
		}
	} else { // specRef is a CommitSpec, which must resolve to a Commit.
		isNonCommitSpec = false
		return isNonCommitSpec, resolvedValue, nil
	}
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

func parseShowArgs(apr *argparser.ArgParseResults) (*showOpts, error) {

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

func printObjects(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts) error {
	if len(opts.specRefs) == 0 {
		headSpec, err := dEnv.RepoStateReader().CWBHeadSpec()
		if err != nil {
			return err
		}

		headRef, err := dEnv.RepoStateReader().CWBHeadRef()
		if err != nil {
			return err
		}

		optCmt, err := dEnv.DoltDB.Resolve(ctx, headSpec, headRef)
		if err != nil {
			return err
		}
		commit, ok := optCmt.ToCommit()
		if !ok {
			return doltdb.ErrUnexpectedGhostCommit // NM4 - TEST.
		}

		value := commit.Value()
		cli.Println(value.Kind(), value.HumanReadableString())
	}

	for _, specRef := range opts.specRefs {
		resolvedValue, ok := opts.resolvedNonCommitSpecs[specRef]
		if !ok {
			return fmt.Errorf("fatal: unable to resolve object ref %s", specRef)
		}
		cli.Println(resolvedValue)
	}

	return nil
}

func printObjectsPretty(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts) error {
	if len(opts.specRefs) == 0 {
		return printCommitSpecPretty(queryist, sqlCtx, opts, "HEAD")
	}

	for _, specRef := range opts.specRefs {
		err := printCommitSpecPretty(queryist, sqlCtx, opts, specRef)
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

func printCommitSpecPretty(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, commitRef string) error {
	if strings.HasPrefix(commitRef, "#") {
		commitRef = strings.TrimPrefix(commitRef, "#")
	}

	commit, err := getCommitInfo(queryist, sqlCtx, commitRef)
	if err != nil {
		return fmt.Errorf("error: failed to get commit metadata for ref '%s': %v", commitRef, err)
	}

	err = printCommit(queryist, sqlCtx, opts, commit)
	if err != nil {
		return err
	}
	return nil
}

func printCommit(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, commit *CommitInfo) error {

	cmHash := commit.commitHash
	parents := commit.parentHashes

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommitInfo(pager, 0, opts.showParents, opts.decoration, commit)
	})

	if len(parents) == 0 {
		return nil
	}
	if len(parents) > 1 {
		return fmt.Errorf("requested commit is a merge commit. 'dolt show' currently only supports viewing non-merge commits")
	}

	parentHash := parents[0]
	datasets := &diffDatasets{
		fromRef: parentHash,
		toRef:   cmHash,
	}

	// An empty string will cause all tables to be printed.
	var tableNames []string

	tableSet, err := parseDiffTableSetSql(queryist, sqlCtx, datasets, tableNames)
	if err != nil {
		return err
	}

	dArgs := &diffArgs{
		diffDisplaySettings: opts.diffDisplaySettings,
		diffDatasets:        datasets,
		tableSet:            tableSet,
	}

	return diffUserTables(queryist, sqlCtx, dArgs)
}
