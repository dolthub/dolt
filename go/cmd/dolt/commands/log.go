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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var logDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show commit logs`,
	LongDesc: `Shows the commit logs

The command takes options to control what is shown and how. 

{{.EmphasisLeft}}dolt log{{.EmphasisRight}}
  Lists commit logs from current HEAD when no options provided.
	
{{.EmphasisLeft}}dolt log [<revisions>...]{{.EmphasisRight}}
  Lists commit logs starting from revision. If multiple revisions provided, lists logs reachable by all revisions.
	
{{.EmphasisLeft}}dolt log [<revisions>...] -- <table>{{.EmphasisRight}}
  Lists commit logs starting from revisions, only including commits with changes to table.
	
{{.EmphasisLeft}}dolt log <revisionB>..<revisionA>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log <revisionA> --not <revisionB>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log ^<revisionB> <revisionA>{{.EmphasisRight}}
  Different ways to list two dot logs. These will list commit logs for revisionA, while excluding commits from revisionB. The table option is not supported for two dot log.
	
{{.EmphasisLeft}}dolt log <revisionB>...<revisionA>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log <revisionA> <revisionB> --not $(dolt merge-base <revisionA> <revisionB>){{.EmphasisRight}}
  Different ways to list three dot logs. These will list commit logs reachable by revisionA OR revisionB, while excluding commits reachable by BOTH revisionA AND revisionB.`,
	Synopsis: []string{
		`[-n {{.LessThan}}num_commits{{.GreaterThan}}] [{{.LessThan}}revision-range{{.GreaterThan}}] [[--] {{.LessThan}}table{{.GreaterThan}}]`,
	},
}

type LogCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LogCmd) Name() string {
	return "log"
}

// Description returns a description of the command
func (cmd LogCmd) Description() string {
	return "Show commit logs."
}

// EventType returns the type of the event to log
func (cmd LogCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LOG
}

func (cmd LogCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(logDocs, ap)
}

func (cmd LogCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateLogArgParser(false)
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv, cliCtx)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, logDocs)
	if terminate {
		return status
	}

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleErrAndExit(err)
	}

	refs, err := getLogRefs(queryist.Queryist, queryist.Context)
	if err != nil {
		return handleErrAndExit(err)
	}

	query, err := constructInterpolatedDoltLogQuery(apr, queryist.Queryist, queryist.Context, refs)
	if err != nil {
		return handleErrAndExit(err)
	}
	// TODO: instead of exhausting the iterator right away, we should incorporate some sort of pagination
	logRows, err := cli.GetRowsForSql(queryist.Queryist, queryist.Context, query)
	if err != nil {
		return handleErrAndExit(err)
	}

	return handleErrAndExit(logCommits(apr, logRows, queryist.Queryist, queryist.Context, refs))
}

// logRefs is the HEAD hash and every branch, remote branch and tag, read with one query and used both to
// classify arguments and to decorate the output.
type logRefs struct {
	headHash string
	// Each map is keyed by commit hash.
	localBranches  map[string][]string
	remoteBranches map[string][]string
	tags           map[string][]string
	hashByName     map[string]string
}

func getLogRefs(queryist cli.Queryist, sqlCtx *sql.Context) (*logRefs, error) {
	rows, err := cli.GetRowsForSql(queryist, sqlCtx, "select 'head', 'HEAD', dolt_hashof('HEAD') "+
		"union all select 'branch', name, hash from dolt_branches "+
		"union all select 'remote', name, hash from dolt_remote_branches "+
		"union all select 'tag', tag_name, tag_hash from dolt_tags")
	if err != nil {
		return nil, fmt.Errorf("error getting refs: %v", err)
	}

	refs := &logRefs{
		localBranches:  make(map[string][]string),
		remoteBranches: make(map[string][]string),
		tags:           make(map[string][]string),
		hashByName:     make(map[string]string),
	}
	for _, row := range rows {
		kind, name, hash := row[0].(string), row[1].(string), row[2].(string)
		refs.hashByName[name] = hash
		switch kind {
		case "head":
			refs.headHash = hash
		case "branch":
			refs.localBranches[hash] = append(refs.localBranches[hash], name)
		case "remote":
			refs.remoteBranches[hash] = append(refs.remoteBranches[hash], name)
		case "tag":
			refs.tags[hash] = append(refs.tags[hash], name)
		}
	}
	return refs, nil
}

// branchHashes returns the commit hash of every local and remote tracking branch.
func (refs *logRefs) branchHashes() []string {
	var hashes []string
	for _, byHash := range []map[string][]string{refs.localBranches, refs.remoteBranches} {
		for hash := range byHash {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

// collectRevisions returns the leading positional arguments that are revisions, and the index of the first table
// argument. Without a -- separator, the revisions end at the first argument that does not resolve to a commit.
func collectRevisions(apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs) ([]string, int, error) {
	if apr.PositionalArgsSeparatorIndex >= 0 {
		return uniqueStrings(apr.Args[:apr.PositionalArgsSeparatorIndex]), apr.PositionalArgsSeparatorIndex, nil
	}

	var revisions []string
	seen := make(map[string]struct{}, apr.NArg())
	tablesIndex := 0
	for _, arg := range apr.Args {
		_, isRepeat := seen[arg]
		if !isRevisionSyntax(arg) {
			// A repeated name is read as a table, so that "dolt log t t" logs table t on revision t.
			if isRepeat {
				break
			}
			isRevision, err := isLogRevision(arg, queryist, sqlCtx, refs)
			if err != nil {
				return nil, 0, err
			}
			if !isRevision {
				break
			}
		}
		tablesIndex++
		if !isRepeat {
			seen[arg] = struct{}{}
			revisions = append(revisions, arg)
		}
	}
	return revisions, tablesIndex, nil
}

// isRevisionSyntax reports whether |arg| is a range, an exclusion or a fully qualified ref, which are revisions
// without needing to be resolved.
func isRevisionSyntax(arg string) bool {
	return strings.Contains(arg, "..") || strings.HasPrefix(arg, "^") || strings.HasPrefix(arg, "refs/") || strings.HasPrefix(arg, "remotes/")
}

// isLogRevision reports whether |arg| resolves to a commit. Known refs are recognized without a query; anything
// else is resolved with dolt_hashof.
func isLogRevision(arg string, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs) (bool, error) {
	if _, ok := refs.hashByName[arg]; ok {
		return true, nil
	}
	q, err := dbr.InterpolateForDialect("select dolt_hashof(?)", []interface{}{arg}, dialect.MySQL)
	if err != nil {
		return false, err
	}
	_, err = cli.GetRowsForSql(queryist, sqlCtx, q)
	return err == nil, nil
}

func uniqueStrings(strs []string) []string {
	var unique []string
	seen := make(map[string]struct{}, len(strs))
	for _, str := range strs {
		if _, ok := seen[str]; !ok {
			seen[str] = struct{}{}
			unique = append(unique, str)
		}
	}
	return unique
}

// collectTables returns the table arguments. Unless they follow a -- separator, each one must exist in one of
// |revisions|, or in HEAD when there are none, or on any branch when --all is given.
func collectTables(apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs, revisions []string, startIndex int) ([]string, error) {
	tableNames := apr.Args[startIndex:]
	if apr.PositionalArgsSeparatorIndex >= 0 {
		return tableNames, nil
	}

	searchRevisions := revisions
	if len(searchRevisions) == 0 || apr.Contains(cli.AllFlag) {
		searchRevisions = append([]string{"HEAD"}, searchRevisions...)
	}
	if apr.Contains(cli.AllFlag) {
		searchRevisions = append(searchRevisions, refs.branchHashes()...)
	}

	missing, err := findMissingTables(tableNames, searchRevisions, queryist, sqlCtx, refs)
	if err != nil {
		return nil, err
	}
	for _, tableName := range tableNames {
		if _, ok := missing[tableName]; ok {
			return nil, fmt.Errorf("error: table %s does not exist", tableName)
		}
	}
	return tableNames, nil
}

// findMissingTables returns the subset of |tableNames| that exists in none of |revisions|. It reads the tables of
// one revision at a time, skips revisions that point at a commit already read, and stops once every table is found.
func findMissingTables(tableNames []string, revisions []string, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs) (map[string]struct{}, error) {
	missing := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		missing[tableName] = struct{}{}
	}

	readCommits := make(map[string]struct{}, len(revisions))
	for _, rev := range revisions {
		if len(missing) == 0 {
			break
		}
		commit := rev
		if hash, ok := refs.hashByName[rev]; ok {
			commit = hash
		}
		if _, ok := readCommits[commit]; ok {
			continue
		}
		readCommits[commit] = struct{}{}

		q, err := dbr.InterpolateForDialect("show tables as of ?", []interface{}{rev}, dialect.MySQL)
		if err != nil {
			return nil, err
		}
		rows, err := cli.GetRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			delete(missing, row[0].(string))
		}
	}
	return missing, nil
}

// constructInterpolatedDoltLogQuery generates the sql query necessary to call the DOLT_LOG() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltLogQuery(apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs) (string, error) {
	var buffer bytes.Buffer
	first := true
	writeToBuffer := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString(s)
		first = false
	}
	buffer.WriteString("select * from dolt_log(")

	// TODO: we can avoid some logic if we make `--table` parameter required like it is in `dolt_log()`
	//  This way, we can easily see which arguments are refs and which are tables without `select hashof(...)`
	params, tablesIndex, err := collectRevisions(apr, queryist, sqlCtx, refs)
	if err != nil {
		return "", err
	}
	for range params {
		writeToBuffer("?")
	}

	if apr.Contains(cli.AllFlag) {
		writeToBuffer("'--all'")
	}

	if tablesIndex < len(apr.Args) {
		tableNames, err := collectTables(apr, queryist, sqlCtx, refs, params, tablesIndex)
		if err != nil {
			return "", err
		}

		if len(tableNames) > 0 {
			params = append(params, strings.Join(tableNames, ","))
			writeToBuffer("'--tables'")
			writeToBuffer("?")
		}
	}

	if minParents, hasMinParents := apr.GetValue(cli.MinParentsFlag); hasMinParents {
		writeToBuffer("?")
		params = append(params, "--min-parents="+minParents)
	}

	if hasMerges := apr.Contains(cli.MergesFlag); hasMerges {
		writeToBuffer("'--merges'")
	}

	if excludedCommits, hasExcludedCommits := apr.GetValueList(cli.NotFlag); hasExcludedCommits {
		writeToBuffer("'--not'")
		for _, commit := range excludedCommits {
			writeToBuffer("?")
			params = append(params, commit)
		}
	}

	if decorate, hasDecorate := apr.GetValue(cli.DecorateFlag); hasDecorate {
		if err := cli.ValidateDecorateOption(decorate); err != nil {
			return "", err
		}
		writeToBuffer("?")
		params = append(params, "--decorate="+resolveDecorateAuto(decorate))
	}

	writeToBuffer("'--parents'")
	if apr.Contains(cli.ShowSignatureFlag) {
		writeToBuffer("'--show-signature'")
	}

	buffer.WriteString(")")

	if numLines, hasNumLines := apr.GetValue(cli.NumberFlag); hasNumLines {
		num, err := strconv.Atoi(numLines)
		if err != nil || num < 0 {
			return "", fmt.Errorf("fatal: invalid --number argument: %s", numLines)
		}
		buffer.WriteString(" limit " + numLines)
	}

	interfaceParams := make([]interface{}, len(params))
	for i, param := range params {
		interfaceParams[i] = param
	}

	interpolatedQuery, err := dbr.InterpolateForDialect(buffer.String(), interfaceParams, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

func logCommits(apr *argparser.ArgParseResults, rows []sql.Row, queryist cli.Queryist, sqlCtx *sql.Context, refs *logRefs) error {

	commitInfos := make([]*CommitInfo, 0, len(rows))
	for _, row := range rows {
		commitHashStr, ok := row[0].(string)
		if !ok {
			return fmt.Errorf("unexpected type for commit hash: %T", row[0])
		}
		committerName, ok := row[1].(string)
		if !ok {
			return fmt.Errorf("unexpected type for committer name: %T", row[1])
		}
		committerEmail, ok := row[2].(string)
		if !ok {
			return fmt.Errorf("unexpected type for committer email: %T", row[2])
		}
		committerDate, err := getTimestampColAsUint64(row[3])
		if err != nil {
			return fmt.Errorf("error parsing committer timestamp '%v': %w", row[3], err)
		}
		commitMessage, ok := row[4].(string)
		if !ok {
			return fmt.Errorf("unexpected type for commit message: %T", row[4])
		}

		var commitOrder uint64
		switch v := row[5].(type) {
		case uint64:
			commitOrder = v
		case string:
			commitOrder, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("error parsing commit_order '%s': %v", v, err)
			}
		default:
			return fmt.Errorf("unexpected type for commit_order: %T", v)
		}

		var parentHashStrs []string
		if parentStr, ok := row[6].(string); ok && parentStr != "" {
			parentHashStrs = strings.Split(parentStr, ", ")
		}

		// signature is NULL when --show-signature was not requested, so a non-string value here is
		// expected; fall through with an empty default.
		var signature string
		if s, ok := row[8].(string); ok {
			signature = s
		}

		authorName, ok := row[9].(string)
		if !ok {
			return fmt.Errorf("unexpected type for author name: %T", row[9])
		}
		authorEmail, ok := row[10].(string)
		if !ok {
			return fmt.Errorf("unexpected type for author email: %T", row[10])
		}
		authorDate, err := getTimestampColAsUint64(row[11])
		if err != nil {
			return fmt.Errorf("error parsing author timestamp '%v': %w", row[11], err)
		}
		commitMeta := &datas.CommitMeta{
			Author: datas.CommitIdent{
				Name:  authorName,
				Email: authorEmail,
				Date:  datas.CommitDateAt(time.UnixMilli(int64(authorDate))),
			},
			Committer: datas.CommitIdent{
				Name:  committerName,
				Email: committerEmail,
				Date:  datas.CommitDateAt(time.UnixMilli(int64(committerDate))),
			},
			Description: commitMessage,
			Signature:   signature,
		}

		localBranches := refs.localBranches[commitHashStr]
		remoteBranches := refs.remoteBranches[commitHashStr]
		tags := refs.tags[commitHashStr]

		commitInfo := &CommitInfo{
			commitMeta:        commitMeta,
			commitHash:        commitHashStr,
			height:            commitOrder,
			isHead:            commitHashStr == refs.headHash,
			parentHashes:      parentHashStrs,
			localBranchNames:  localBranches,
			remoteBranchNames: remoteBranches,
			tagNames:          tags,
		}
		commitInfos = append(commitInfos, commitInfo)
	}

	// Resolve auto before opening the pager. checkIsTerminal uses ExecuteWithStdioRestored,
	// which mutates os.Stdout, and the pager block calls ExecuteWithStdioRestored too.
	// Calling it from inside the pager block leaves the pager holding a stale stdout handle.
	decoration := resolveDecorateAuto(apr.GetValueOrDefault(cli.DecorateFlag, cli.DecorateAuto))
	return logToStdOut(apr, commitInfos, sqlCtx, queryist, decoration)
}

// resolveDecorateAuto returns DecorateShort when stdout is a terminal and DecorateNo
// otherwise. Other |decorate| values pass through unchanged.
func resolveDecorateAuto(decorate string) string {
	if decorate != cli.DecorateAuto {
		return decorate
	}
	if checkIsTerminal() {
		return cli.DecorateShort
	}
	return cli.DecorateNo
}

func logCompact(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []*CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist, decoration string) error {
	color.NoColor = false
	for _, comm := range commits {
		if len(comm.parentHashes) < apr.GetIntOrDefault(cli.MinParentsFlag, 0) {
			return nil
		}

		chStr := comm.commitHash
		if apr.Contains(cli.ParentsFlag) {
			for _, h := range comm.parentHashes {
				chStr += " " + h
			}
		}

		// TODO: use short hash instead
		// Write commit hash
		pager.Writer.Write([]byte(color.YellowString("%s ", chStr)))

		if decoration != cli.DecorateNo {
			printRefs(pager, comm, decoration)
		}

		formattedDesc := strings.Replace(comm.commitMeta.Description, "\n", " ", -1) + "\n"
		pager.Writer.Write([]byte(formattedDesc))

		if apr.Contains(cli.StatFlag) {
			if comm.parentHashes != nil && len(comm.parentHashes) == 1 { // don't print stats for merge commits
				diffStats := make(map[string]*merge.MergeStats)
				diffStats, _, err := calculateMergeStats(queryist, sqlCtx, diffStats, comm.parentHashes[0], comm.commitHash)
				if err != nil {
					return err
				}
				printDiffStats(diffStats, pager)
			}
		}
	}

	return nil
}

func logDefault(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []*CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist, decoration string) error {
	for _, comm := range commits {
		PrintCommitInfo(pager, apr.GetIntOrDefault(cli.MinParentsFlag, 0), apr.Contains(cli.ParentsFlag), apr.Contains(cli.ShowSignatureFlag), decoration, comm)
		if apr.Contains(cli.StatFlag) {
			if comm.parentHashes != nil && len(comm.parentHashes) == 1 { // don't print stats for merge commits
				diffStats := make(map[string]*merge.MergeStats)
				diffStats, _, err := calculateMergeStats(queryist, sqlCtx, diffStats, comm.parentHashes[0], comm.commitHash)
				if err != nil {
					return err
				}
				printDiffStats(diffStats, pager)
				pager.Writer.Write([]byte("\n"))
			}
		}
	}

	return nil
}

func logToStdOut(apr *argparser.ArgParseResults, commits []*CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist, decoration string) (err error) {
	if cli.ExecuteWithStdioRestored == nil {
		return nil
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()
		if apr.Contains(cli.GraphFlag) {
			logGraph(pager, apr, commits)
		} else if apr.Contains(cli.OneLineFlag) {
			err = logCompact(pager, apr, commits, sqlCtx, queryist, decoration)
		} else {
			err = logDefault(pager, apr, commits, sqlCtx, queryist, decoration)
		}
	})

	return
}

// printDiffStats prints the diff stats for a commit to a pager
func printDiffStats(diffStats map[string]*merge.MergeStats, pager *outputpager.Pager) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableModified {
			tbls = append(tbls, tblName)
			nameLen := len(tblName)
			modCount := stats.Adds + stats.Modifications + stats.Deletes

			if nameLen > maxNameLen {
				maxNameLen = nameLen
			}

			if modCount > maxModCount {
				maxModCount = modCount
			}

			rowsAdded += stats.Adds
			rowsChanged += stats.Modifications
			rowsDeleted += stats.Deletes
		}
	}

	if len(tbls) > 0 {
		sort.Strings(tbls)
		modCountStrLen := len(strconv.FormatInt(int64(maxModCount), 10))
		format := fmt.Sprintf(" %%-%ds | %%-%ds %%s\n", maxNameLen, modCountStrLen)

		for _, tbl := range tbls {
			stats := diffStats[tbl]
			if stats.Operation == merge.TableModified {
				modCount := stats.Adds + stats.Modifications + stats.Deletes
				modCountStr := strconv.FormatInt(int64(modCount), 10)
				visualizedChanges := visualizeChangesForLog(stats, maxModCount)

				pager.Writer.Write([]byte(fmt.Sprintf(format, tbl, modCountStr, visualizedChanges)))
			}
		}

		details := fmt.Sprintf(" %d tables changed, %d rows added(+), %d rows modified(*), %d rows deleted(-)\n", len(tbls), rowsAdded, rowsChanged, rowsDeleted)
		pager.Writer.Write([]byte(details))
	}

	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableAdded {
			pager.Writer.Write([]byte(" " + tblName + " added\n"))
		}
	}
	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableRemoved {
			pager.Writer.Write([]byte(" " + tblName + " deleted\n"))
		}
	}
}

// visualizeChangesForLog generates the string with the appropriate symbols to represent the changes in a commit with
// the corresponding color suitable for writing to a pager
func visualizeChangesForLog(stats *merge.MergeStats, maxMods int) string {
	color.NoColor = false
	const maxVisLen = 30 //can be a bit longer due to min len and rounding

	resultStr := ""
	if stats.Adds > 0 {
		addLen := int(maxVisLen * (float64(stats.Adds) / float64(maxMods)))
		if addLen > stats.Adds {
			addLen = stats.Adds
		}
		addStr := fillStringWithChar('+', addLen)
		resultStr += color.HiGreenString("%s", addStr)

	}

	if stats.Modifications > 0 {
		modLen := int(maxVisLen * (float64(stats.Modifications) / float64(maxMods)))
		if modLen > stats.Modifications {
			modLen = stats.Modifications
		}
		modStr := fillStringWithChar('*', modLen)
		resultStr += color.HiYellowString("%s", modStr)
	}

	if stats.Deletes > 0 {
		delLen := int(maxVisLen * (float64(stats.Deletes) / float64(maxMods)))
		if delLen > stats.Deletes {
			delLen = stats.Deletes
		}
		delStr := fillStringWithChar('-', delLen)
		resultStr += color.HiRedString("%s", delStr)
	}

	return resultStr
}

func handleErrAndExit(err error) int {
	if err != nil {
		cli.PrintErrln(strings.ReplaceAll(err.Error(), "Invalid argument to dolt_log: ", ""))
		return 1
	}

	return 0
}
