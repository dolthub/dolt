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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var hashRegex = regexp.MustCompile(`^#?[0-9a-v]{32}$`)

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

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	resolvedRefs := make([]string, 0, len(opts.specRefs))
	for _, specRef := range opts.specRefs {
		if !hashRegex.MatchString(specRef) &&
			!strings.EqualFold(specRef, "HEAD") &&
			!strings.EqualFold(specRef, "WORKING") &&
			!strings.EqualFold(specRef, "STAGED") {
			// Call "dolt_hashof" to resolve the ref to a hash. the --no-pretty flag gets around the commit requirement, but
			// requires the full object name so it will match the hashRegex and never hit this code block.
			h, err2 := getHashOf(queryist, sqlCtx, specRef)
			if err2 != nil {
				cli.PrintErrln(fmt.Sprintf("branch not found: %s", specRef))
				return 1
			}
			resolvedRefs = append(resolvedRefs, h)
		} else {
			resolvedRefs = append(resolvedRefs, specRef)
		}
	}
	if len(resolvedRefs) == 0 {
		resolvedRefs = []string{"HEAD"}
	}

	// There are two response formats:
	//  - "pretty", which shows commits in a human-readable fashion
	//  - "raw", which shows the underlying SerialMessage
	// All responses should be in the same format.
	// The pretty format is preferred unless the --no-pretty flag is provided.
	// But only commits are supported in the "pretty" format.
	// Thus, if the --no-pretty flag is not set, then we require that either all the references are commits, or none of them are.
	if !opts.pretty {
		if dEnv == nil {
			// Logic below requires a non-nil dEnv when --no-pretty is set.
			// TODO: remove all usage of dEnv entirely from this command.
			cli.PrintErrln("`\\show --no-pretty` is not supported in the SQL shell.")
			return 1
		}

		// use dEnv instead of the SQL engine
		_, ok := queryist.(*engine.SqlEngine)
		if !ok {
			cli.PrintErrln("`dolt show --no-pretty` or `dolt show (BRANCHNAME)` only supported in local mode.")
			return 1
		}

		if !opts.pretty && !dEnv.DoltDB.Format().UsesFlatbuffers() {
			cli.PrintErrln("`dolt show --no-pretty` or `dolt show (BRANCHNAME)` is not supported when using old LD_1 storage format.")
			return 1
		}
	}

	for _, specRef := range resolvedRefs {
		// If --no-pretty was supplied, always display the raw contents of the referenced object.
		if !opts.pretty {
			err := printRawValue(ctx, dEnv, specRef)
			if err != nil {
				return handleErrAndExit(err)
			}
			continue
		}

		// If the argument is a commit, display it in the "pretty" format.
		// But if it's a hash, we don't know whether it's a commit until we query the engine. If a non-commit was specified
		// by the user, we are forced to display the raw contents of the object - which required a dEnv
		commitInfo, err := getCommitSpecPretty(queryist, sqlCtx, specRef)
		if commitInfo == nil {
			// Hash is not a commit
			_, ok := queryist.(*engine.SqlEngine)
			if !ok {
				cli.PrintErrln("`dolt show (NON_COMMIT_HASH)` only supported in local mode.")
				return 1
			}
			if dEnv == nil {
				cli.PrintErrln("`dolt show (NON_COMMIT_HASH)` requires a local environment. Not intended for common use.")
				return 1
			}
			if !dEnv.DoltDB.Format().UsesFlatbuffers() {
				cli.PrintErrln("`dolt show (NON_COMMIT_HASH)` is not supported when using old LD_1 storage format.")
				return 1
			}

			value, err := getValueFromRefSpec(ctx, dEnv, specRef)
			if err != nil {
				err = fmt.Errorf("error resolving spec ref '%s': %w", specRef, err)
				if err != nil {
					return handleErrAndExit(err)
				}
			}
			cli.Println(value.Kind(), value.HumanReadableString())
			continue
		} else {
			// Hash is a commit
			err = fetchAndPrintCommit(queryist, sqlCtx, opts, commitInfo)
			if err != nil {
				return handleErrAndExit(err)
			}
			continue
		}
	}
	return 0
}

// printRawValue prints the raw value of the object referenced by specRef. This function works directly on storage, and
// requires a non-nil dEnv.
func printRawValue(ctx context.Context, dEnv *env.DoltEnv, specRef string) error {
	value, err := getValueFromRefSpec(ctx, dEnv, specRef)
	if err != nil {
		return fmt.Errorf("error resolving spec ref '%s': %w", specRef, err)
	}
	cli.Println(value.Kind(), value.HumanReadableString())
	return nil
}

// getValueFromRefSpec returns the value of the object referenced by specRef. This
// function works directly on storage, and requires a non-nil dEnv.
func getValueFromRefSpec(ctx context.Context, dEnv *env.DoltEnv, specRef string) (types.Value, error) {
	var refHash hash.Hash
	var err error
	roots, err := dEnv.Roots(ctx)
	if strings.EqualFold(specRef, doltdb.Working) {
		refHash, err = roots.Working.HashOf()
	} else if strings.EqualFold(specRef, doltdb.Staged) {
		refHash, err = roots.Staged.HashOf()
	} else if hashRegex.MatchString(specRef) {
		refHash, err = parseHashString(specRef)
	} else {
		commitSpec, err := doltdb.NewCommitSpec(specRef)
		if err != nil {
			return nil, err
		}
		headRef, err := dEnv.RepoStateReader().CWBHeadRef()
		optionalCommit, err := dEnv.DoltDB.Resolve(ctx, commitSpec, headRef)
		if err != nil {
			return nil, err
		}
		commit, ok := optionalCommit.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		return commit.Value(), nil
	}
	if err != nil {
		return nil, err
	}
	value, err := dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, refHash)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("Unable to resolve object ref %s", specRef)
	}
	return value, nil
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

// parseHashString converts a string representing a hash into a hash.Hash.
func parseHashString(hashStr string) (hash.Hash, error) {
	unprefixed := strings.TrimPrefix(hashStr, "#")
	parsedHash, ok := hash.MaybeParse(unprefixed)
	if !ok {
		return hash.Hash{}, errors.New("invalid hash: " + hashStr)
	}
	return parsedHash, nil
}

func getCommitSpecPretty(queryist cli.Queryist, sqlCtx *sql.Context, commitRef string) (commit *CommitInfo, err error) {
	if strings.HasPrefix(commitRef, "#") {
		commitRef = strings.TrimPrefix(commitRef, "#")
	}

	commit, err = getCommitInfo(queryist, sqlCtx, commitRef)
	if err != nil {
		return commit, fmt.Errorf("error: failed to get commit metadata for ref '%s': %v", commitRef, err)
	}
	return
}

func fetchAndPrintCommit(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, commit *CommitInfo) error {

	cmHash := commit.commitHash
	parents := commit.parentHashes

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommitInfo(pager, 0, opts.showParents, false, opts.decoration, commit)
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
