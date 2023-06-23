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
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/pkg/errors"
	"regexp"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

var hashRegex = regexp.MustCompile(`^#?[0-9a-v]{32}$`)

type commitInfo struct {
	commitMeta   *datas.CommitMeta
	commitHash   string
	isHead       bool
	parentHashes []string
	height       uint64
	branchNames  []string
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

	// TODO: pavel - is this check still needed in a SQL-only world?
	//if !opts.pretty && !dEnv.DoltDB.Format().UsesFlatbuffers() {
	//	cli.PrintErrln("dolt show --no-pretty is not supported when using old LD_1 storage format.")
	//	return 1
	//}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	opts.diffDisplaySettings = parseDiffDisplaySettings(apr)

	err = showObjects(queryist, sqlCtx, opts)

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

func showObjects(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts) error {
	if len(opts.specRefs) == 0 {
		//headRef, err := dEnv.RepoStateReader().CWBHeadSpec()
		//if err != nil {
		//	return err
		//}
		return showCommitSpec(queryist, sqlCtx, opts, "HEAD")
	}

	for _, specRef := range opts.specRefs {
		//err := showSpecRef(queryist, sqlCtx, opts, specRef)
		err := showCommitSpec(queryist, sqlCtx, opts, specRef)
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

//func showSpecRef(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, specRef string) error {
//	roots, err := dEnv.Roots(ctx)
//	if err != nil {
//		return err
//	}
//
//	upperCaseSpecRef := strings.ToUpper(specRef)
//	if upperCaseSpecRef == doltdb.Working || upperCaseSpecRef == doltdb.Staged || hashRegex.MatchString(specRef) {
//		var refHash hash.Hash
//		var err error
//		if upperCaseSpecRef == doltdb.Working {
//			refHash, err = roots.Working.HashOf()
//		} else if upperCaseSpecRef == doltdb.Staged {
//			refHash, err = roots.Staged.HashOf()
//		} else {
//			refHash, err = parseHashString(specRef)
//		}
//		if err != nil {
//			return err
//		}
//		value, err := dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, refHash)
//		if err != nil {
//			return err
//		}
//		if value == nil {
//			return fmt.Errorf("Unable to resolve object ref %s", specRef)
//		}
//
//		if !opts.pretty {
//			cli.Println(value.Kind(), value.HumanReadableString())
//		}
//
//		// If this is a commit, use the pretty printer. To determine whether it's a commit, try calling NewCommitFromValue.
//		commit, err := doltdb.NewCommitFromValue(ctx, dEnv.DoltDB.ValueReadWriter(), dEnv.DoltDB.NodeStore(), value)
//
//		if err == datas.ErrNotACommit {
//			if !dEnv.DoltDB.Format().UsesFlatbuffers() {
//				return fmt.Errorf("dolt show cannot show non-commit objects when using the old LD_1 storage format: %s is not a commit", specRef)
//			}
//			cli.Println(value.Kind(), value.HumanReadableString())
//		} else if err == nil {
//			showCommit(ctx, dEnv, opts, commit)
//		} else {
//			return err
//		}
//	} else { // specRef is a CommitSpec, which must resolve to a Commit.
//		commitSpec, err := getCommitSpec(specRef)
//		if err != nil {
//			return err
//		}
//
//		err = showCommitSpec(ctx, dEnv, opts, commitSpec)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

func showCommitSpec(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, commitRef string) error {

	if opts.pretty {
		err := showCommit(queryist, sqlCtx, opts, commitRef)
		if err != nil {
			return err
		}
	} else {
		commit, err := getCommitInfo(queryist, sqlCtx, commitRef)
		if err != nil {
			cli.PrintErrln("error: failed to get commit metadata for ref:", commitRef)
			return err
		}
		meta := commit.commitMeta
		cmHash := commit.commitHash
		parents := commit.parentHashes
		commitDate := time.UnixMilli(int64(meta.Timestamp))

		sb := strings.Builder{}
		sb.WriteString("{\n")
		sb.WriteString(fmt.Sprintf("\tName: %s\n", meta.Name))
		sb.WriteString(fmt.Sprintf("\tDesc: %s\n", meta.Description))
		sb.WriteString(fmt.Sprintf("\tEmail: %s\n", meta.Email))
		sb.WriteString(fmt.Sprintf("\tDate: %s\n", commitDate.String()))
		sb.WriteString(fmt.Sprintf("\tHeight: %d\n", commit.height))
		sb.WriteString(fmt.Sprintf("\tRootValue: {\n"))
		sb.WriteString(fmt.Sprintf("\t\t#%s\n", cmHash))
		sb.WriteString(fmt.Sprintf("\t}\n"))
		if len(parents) > 0 {
			sb.WriteString(fmt.Sprintf("\tParents: {\n"))
			for _, p := range parents {
				sb.WriteString(fmt.Sprintf("\t\t#%s\n", p))
			}
			sb.WriteString(fmt.Sprintf("\t}\n"))
		}
		sb.WriteString("}")
		cli.Println(sb.String())
	}
	return nil
}

func showCommit(queryist cli.Queryist, sqlCtx *sql.Context, opts *showOpts, ref string) error {

	//cHashToRefs, err := getHashToRefs(ctx, dEnv, opts.decoration)
	//if err != nil {
	//	return err
	//}

	commit, err := getCommitInfo(queryist, sqlCtx, ref)
	if err != nil {
		cli.PrintErrln("error: failed to get commit metadata for ref:", ref)
		return err
	}
	cmHash := commit.commitHash
	parents := commit.parentHashes

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		showCommitInfo(pager, 0, opts.showParents, opts.decoration, commit)
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

func showCommitInfo(pager *outputpager.Pager, minParents int, showParents bool, decoration string, comm *commitInfo) {

	if len(comm.parentHashes) < minParents {
		return
	}

	chStr := comm.commitHash
	if showParents {
		for _, h := range comm.parentHashes {
			chStr += " " + h
		}
	}

	// Write commit hash
	pager.Writer.Write([]byte(fmt.Sprintf("\033[33mcommit %s \033[0m", chStr))) // Use Dim Yellow (33m)

	// Show decoration
	if decoration != "no" {
		showRefs(pager, comm)
	}

	if len(comm.parentHashes) > 1 {
		pager.Writer.Write([]byte(fmt.Sprintf("\nMerge:")))
		for _, h := range comm.parentHashes {
			pager.Writer.Write([]byte(fmt.Sprintf(" " + h)))
		}
	}

	pager.Writer.Write([]byte(fmt.Sprintf("\nAuthor: %s <%s>", comm.commitMeta.Name, comm.commitMeta.Email)))

	timeStr := comm.commitMeta.FormatTS()
	pager.Writer.Write([]byte(fmt.Sprintf("\nDate:  %s", timeStr)))

	formattedDesc := "\n\n\t" + strings.Replace(comm.commitMeta.Description, "\n", "\n\t", -1) + "\n\n"
	pager.Writer.Write([]byte(fmt.Sprintf("%s", formattedDesc)))

}

func showRefs(pager *outputpager.Pager, comm *commitInfo) {
	// Do nothing if no associate branchNames
	if len(comm.branchNames) == 0 {
		return
	}

	pager.Writer.Write([]byte("\033[33m(\033[0m"))
	if comm.isHead {
		pager.Writer.Write([]byte("\033[36;1mHEAD -> \033[0m"))
	}
	pager.Writer.Write([]byte(strings.Join(comm.branchNames, "\033[33m, \033[0m"))) // Separate with Dim Yellow comma
	pager.Writer.Write([]byte("\033[33m) \033[0m"))
}

func getCommitInfo(queryist cli.Queryist, sqlCtx *sql.Context, ref string) (*commitInfo, error) {
	hashOfHead, err := getHashOf(queryist, sqlCtx, "HEAD")
	if err != nil {
		return nil, fmt.Errorf("error getting hash of HEAD: %v", err)
	}

	q := fmt.Sprintf("select * from dolt_log('%s', '--parents', '--decorate=full')", ref)
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, fmt.Errorf("error getting logs for ref '%s': %v", ref, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no commits found for ref %s", ref)
	}

	row := rows[0]
	commitHash := row[0].(string)
	name := row[1].(string)
	email := row[2].(string)
	timestamp, err := getTimestampColAsUint64(row[3])
	if err != nil {
		return nil, fmt.Errorf("error parsing timestamp '%s': %v", err)
	}
	message := row[4].(string)
	parent := row[5].(string)
	height := uint64(len(rows))
	isHead := commitHash == hashOfHead

	localBranchesForHash, err := getBranchesForHash(queryist, sqlCtx, commitHash, true)
	if err != nil {
		return nil, err
	}
	remoteBranchesForHash, err := getBranchesForHash(queryist, sqlCtx, commitHash, false)
	if err != nil {
		return nil, err
	}
	branches := []string{}
	branches = append(branches, localBranchesForHash...)
	branches = append(branches, remoteBranchesForHash...)

	ci := &commitInfo{
		commitMeta: &datas.CommitMeta{
			Name:          name,
			Email:         email,
			Timestamp:     timestamp,
			Description:   message,
			UserTimestamp: int64(timestamp),
		},
		commitHash:  commitHash,
		height:      height,
		isHead:      isHead,
		branchNames: branches,
	}

	if parent != "" {
		ci.parentHashes = []string{parent}
	}

	return ci, nil
}

func getBranchesForHash(queryist cli.Queryist, sqlCtx *sql.Context, targetHash string, getLocalBranches bool) ([]string, error) {
	var q string
	if getLocalBranches {
		q = fmt.Sprintf("select name, hash from dolt_branches where hash = '%s'", targetHash)
	} else {
		q = fmt.Sprintf("select name, hash from dolt_remote_branches where hash = '%s'", targetHash)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	branches := []string{}
	for _, row := range rows {
		name := row[0].(string)
		branches = append(branches, name)
	}
	return branches, nil
}

func getHashOf(queryist cli.Queryist, sqlCtx *sql.Context, ref string) (string, error) {
	q := fmt.Sprintf("select hashof('%s')", ref)
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", fmt.Errorf("no commits found for ref %s", ref)
	}
	return rows[0][0].(string), nil
}

func getTimestampColAsUint64(col interface{}) (uint64, error) {
	switch v := col.(type) {
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case time.Time:
		return uint64(v.UnixMilli()), nil
	default:
		return 0, fmt.Errorf("unexpected type %T, was expecting int64, uint64 or time.Time", v)
	}
}
