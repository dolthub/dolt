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
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx *cli.CliContext) int {
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

	opts.diffDisplaySettings = parseDiffDisplaySettings(ctx, dEnv, apr)

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
		return showCommitSpec(ctx, dEnv, opts, dEnv.RepoStateReader().CWBHeadSpec())
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

	commit, err := dEnv.DoltDB.Resolve(ctx, commitSpec, dEnv.RepoStateReader().CWBHeadRef())
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

	headRef := dEnv.RepoStateReader().CWBHeadRef()
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

	datasets := &diffDatasets{
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

	dArgs := &diffArgs{
		diffDisplaySettings: opts.diffDisplaySettings,
		diffDatasets:        datasets,
		tableSet:            tableSet,
	}

	return diffUserTables(ctx, dEnv, dArgs)
}
