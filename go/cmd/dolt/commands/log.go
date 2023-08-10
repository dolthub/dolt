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
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

type logOpts struct {
	numLines             int
	showParents          bool
	minParents           int
	decoration           string
	oneLine              bool
	excludingCommitSpecs []*doltdb.CommitSpec
	commitSpecs          []*doltdb.CommitSpec
	tableName            string
}

type logNode struct {
	commitMeta   *datas.CommitMeta
	commitHash   hash.Hash
	parentHashes []hash.Hash
	branchNames  []string
	isHead       bool
}

var logDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show commit logs`,
	LongDesc: `Shows the commit logs

The command takes options to control what is shown and how. 

{{.EmphasisLeft}}dolt log{{.EmphasisRight}}
  Lists commit logs from current HEAD when no options provided.
	
{{.EmphasisLeft}}dolt log [<revisions>...]{{.EmphasisRight}}
  Lists commit logs starting from revision. If multiple revisions provided, lists logs reachable by all revisions.
	
{{.EmphasisLeft}}dolt log [<revisions>...] <table>{{.EmphasisRight}}
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
	return cli.CreateLogArgParser()
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv, cliCtx)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, logDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		handleErrAndExit(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	opts, err := parseLogArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	query, err := constructInterpolatedDoltLogQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	logRows, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return handleErrAndExit(err)
	}

	/*if len(opts.commitSpecs) == 0 {
		headRef, err := dEnv.RepoStateReader().CWBHeadSpec()
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		opts.commitSpecs = append(opts.commitSpecs, headRef)
	}
	if len(opts.tableName) > 0 {
		return handleErrAndExit(logTableCommits(ctx, dEnv, opts))
	}*/
	return logCommits(opts, apr, logRows, queryist, sqlCtx)
}

// constructInterpolatedDoltLogQuery generates the sql query necessary to call the DOLT_LOG() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltLogQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}

	var buffer bytes.Buffer
	var first bool
	first = true

	buffer.WriteString("select commit_hash, committer, email, date, message, parents, refs from dolt_log(")

	writeToBuffer := func(s string, param bool) {
		if !first {
			buffer.WriteString(", ")
		}
		if !param {
			buffer.WriteString("'")
		}
		buffer.WriteString(s)
		if !param {
			buffer.WriteString("'")
		}
		first = false
	}

	for _, args := range apr.Args {
		writeToBuffer("?", true)
		params = append(params, args)
	}

	if minParents, hasMinParents := apr.GetValue(cli.MinParentsFlag); hasMinParents {
		writeToBuffer("?", true)
		params = append(params, "--min-parents="+minParents)
	}

	if hasMerges := apr.Contains(cli.MergesFlag); hasMerges {
		writeToBuffer("--merges", false)
	}

	if excludedCommits, hasExcludedCommits := apr.GetValueList(cli.NotFlag); hasExcludedCommits {
		writeToBuffer("--not", false)
		for _, commit := range excludedCommits {
			writeToBuffer("?", true)
			params = append(params, commit)
		}
	}

	writeToBuffer("--parents", false)
	writeToBuffer("--decorate=full", false)
	buffer.WriteString(")")

	if numLines, hasNumLines := apr.GetValue(cli.NumberFlag); hasNumLines {
		num, err := strconv.Atoi(numLines)
		if err != nil || num < 0 {
			return "", fmt.Errorf("fatal: invalid --number argument: %s", numLines)
		}
		buffer.WriteString(" limit " + numLines)
	}

	interpolatedQuery, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

func parseLogArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*logOpts, error) {
	minParents := apr.GetIntOrDefault(cli.MinParentsFlag, 0)
	if apr.Contains(cli.MergesFlag) {
		minParents = 2
	}

	decorateOption := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return nil, fmt.Errorf("fatal: invalid --decorate option: %s", decorateOption)
	}

	opts := &logOpts{
		numLines:    apr.GetIntOrDefault(cli.NumberFlag, -1),
		showParents: apr.Contains(cli.ParentsFlag),
		minParents:  minParents,
		oneLine:     apr.Contains(cli.OneLineFlag),
		decoration:  decorateOption,
	}

	err := opts.parseRefsAndTable(ctx, apr, dEnv)
	if err != nil {
		return nil, err
	}

	excludingRefs, ok := apr.GetValueList(cli.NotFlag)
	if ok {
		if len(opts.excludingCommitSpecs) > 0 {
			return nil, fmt.Errorf("cannot use --not argument with two dots or ref with ^")
		}
		if len(opts.tableName) > 0 {
			return nil, fmt.Errorf("cannot use --not argument with table")
		}
		for _, excludingRef := range excludingRefs {
			notCs, err := doltdb.NewCommitSpec(excludingRef)
			if err != nil {
				return nil, fmt.Errorf("invalid commit %s\n", excludingRef)
			}

			opts.excludingCommitSpecs = append(opts.excludingCommitSpecs, notCs)
		}
	}

	return opts, nil
}

func (opts *logOpts) parseRefsAndTable(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) error {
	// `dolt log`
	if apr.NArg() == 0 {
		return nil
	}

	if strings.Contains(apr.Arg(0), "..") {
		if apr.NArg() > 1 {
			return fmt.Errorf("Cannot use two or three dot syntax when 2 or more arguments provided")
		}

		// `dolt log <ref>...<ref>`
		if strings.Contains(apr.Arg(0), "...") {
			refs := strings.Split(apr.Arg(0), "...")

			for _, ref := range refs {
				cs, err := getCommitSpec(ref)
				if err != nil {
					return err
				}
				opts.commitSpecs = append(opts.commitSpecs, cs)
			}

			mergeBase, verr := getMergeBaseFromStrings(ctx, dEnv, refs[0], refs[1])
			if verr != nil {
				return verr
			}
			notCs, err := getCommitSpec(mergeBase)
			if err != nil {
				return err
			}
			opts.excludingCommitSpecs = append(opts.excludingCommitSpecs, notCs)

			return nil
		}

		// `dolt log <ref>..<ref>`
		refs := strings.Split(apr.Arg(0), "..")
		notCs, err := getCommitSpec(refs[0])
		if err != nil {
			return err
		}

		cs, err := getCommitSpec(refs[1])
		if err != nil {
			return err
		}

		opts.commitSpecs = append(opts.commitSpecs, cs)
		opts.excludingCommitSpecs = append(opts.excludingCommitSpecs, notCs)

		return nil
	}

	seenRefs := make(map[string]bool)

	for _, arg := range apr.Args {
		// ^<ref>
		if strings.HasPrefix(arg, "^") {
			commit := strings.TrimPrefix(arg, "^")
			notCs, err := getCommitSpec(commit)
			if err != nil {
				return err
			}

			opts.excludingCommitSpecs = append(opts.excludingCommitSpecs, notCs)
		} else {
			argIsRef, err := actions.IsValidRef(ctx, arg, dEnv.DoltDB, dEnv.RepoStateReader())
			if err != nil {
				return nil
			}
			// <ref>
			if argIsRef && !seenRefs[arg] {
				cs, err := getCommitSpec(arg)
				if err != nil {
					return err
				}
				seenRefs[arg] = true
				opts.commitSpecs = append(opts.commitSpecs, cs)
			} else {
				// <table>
				opts.tableName = arg
			}
		}
	}

	if len(opts.tableName) > 0 && len(opts.excludingCommitSpecs) > 0 {
		return fmt.Errorf("Cannot provide table name with excluding refs")
	}

	return nil
}

func getCommitSpec(commit string) (*doltdb.CommitSpec, error) {
	cs, err := doltdb.NewCommitSpec(commit)
	if err != nil {
		return nil, fmt.Errorf("invalid commit %s\n", commit)
	}
	return cs, nil
}

func getHashToRefs(decorationLevel string, queryist cli.Queryist, sqlCtx *sql.Context) (map[hash.Hash][]string, error) {
	cHashToRefs := map[hash.Hash][]string{}

	// Get all branches
	branches, err := GetRowsForSql(queryist, sqlCtx, "select name, hash from dolt_branches")
	if err != nil {
		return cHashToRefs, fmt.Errorf(color.HiRedString("Fatal error: cannot get Branch information."))
	}
	for _, b := range branches {
		refName := b[0].(string)
		/*if decorationLevel != "full" {
			refName = b.Ref.GetPath() // trim out "refs/heads/"
		}*/
		refName = fmt.Sprintf("\033[32;1m%s\033[0m", refName) // branch names are bright green (32;1m)
		cHashToRefs[hash.Parse(b[1].(string))] = append(cHashToRefs[hash.Parse(b[1].(string))], refName)
	}

	// Get all remote branches
	remotes, err := GetRowsForSql(queryist, sqlCtx, "select name, hash from dolt_remote_branches")
	if err != nil {
		return cHashToRefs, fmt.Errorf(color.HiRedString("Fatal error: cannot get Remotes information."))
	}
	for _, r := range remotes {
		refName := r[0].(string)
		/*if decorationLevel != "full" {
			refName = r.Ref.GetPath() // trim out "refs/remotes/"
		}*/
		refName = fmt.Sprintf("\033[31;1m%s\033[0m", refName) // remote names are bright red (31;1m)
		cHashToRefs[hash.Parse(r[1].(string))] = append(cHashToRefs[hash.Parse(r[1].(string))], refName)
	}

	// Get all tags
	tags, err := GetRowsForSql(queryist, sqlCtx, "select tag_name, tag_hash from dolt_tags")
	if err != nil {
		return cHashToRefs, fmt.Errorf(color.HiRedString("Fatal error: cannot get Tag information."))
	}
	for _, t := range tags {
		tagName := t[0].(string)
		/*if decorationLevel != "full" {
			tagName = t.Tag.Name // trim out "refs/tags/"
		}*/
		tagName = fmt.Sprintf("\033[33;1mtag: %s\033[0m", tagName) // tags names are bright yellow (33;1m)
		cHashToRefs[hash.Parse(t[1].(string))] = append(cHashToRefs[hash.Parse(t[1].(string))], tagName)
	}
	return cHashToRefs, nil
}

func logCommits(opts *logOpts, apr *argparser.ArgParseResults, sqlResult []sql.Row, queryist cli.Queryist, sqlCtx *sql.Context) int {
	/*matchFunc := func(c *doltdb.Commit) (bool, error) {
		return c.NumParents() >= opts.minParents, nil
	}*/

	/*var commits []*doltdb.Commit
	if len(opts.excludingCommitSpecs) == 0 {
		commits, err = commitwalk.GetTopNTopoOrderedCommitsMatching(ctx, dEnv.DoltDB, hashes, opts.numLines, matchFunc)
	} else {
		excludingHashes := make([]hash.Hash, len(opts.excludingCommitSpecs))

		for i, excludingSpec := range opts.excludingCommitSpecs {
			excludingCommit, err := dEnv.DoltDB.Resolve(ctx, excludingSpec, headRef)
			if err != nil {
				cli.PrintErrln(color.HiRedString("Fatal error: cannot get excluding commit for current branch."))
				return 1
			}

			excludingHash, err := excludingCommit.HashOf()
			if err != nil {
				cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
				return 1
			}

			excludingHashes[i] = excludingHash
		}

		commits, err = commitwalk.GetDotDotRevisions(ctx, dEnv.DoltDB, hashes, dEnv.DoltDB, excludingHashes, opts.numLines)
	}

	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	cwbHash, err := dEnv.DoltDB.GetHashForRefStr(ctx, headRef.String())

	if err != nil {
		cli.PrintErrln(err)
		return 1
	}*/

	/*cHashToRefs, err := getHashToRefs(opts.decoration, queryist, sqlCtx)
	if err != nil {
		return handleErrAndExit(err)
	}*/

	var commitsInfo []logNode
	for _, row := range sqlResult {
		name := row[1].(string)
		email := row[2].(string)
		timestamp := uint64(row[3].(time.Time).Unix())
		description := row[4].(string)

		var parents []hash.Hash
		parentStrings := strings.Split(row[5].(string), ", ")
		for _, parentString := range parentStrings {
			if parentString != "" {
				parents = append(parents, hash.Parse(parentString))
			}
		}
		refs := strings.Split(row[6].(string), ", ")

		meta := &datas.CommitMeta{
			Name:          name,
			Email:         email,
			Timestamp:     timestamp,
			Description:   description,
			UserTimestamp: 0,
		}

		headResult, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select @@%s_head", sqlCtx.GetCurrentDatabase()))
		if err != nil {
			return handleErrAndExit(err)
		}
		headHash := hash.Parse(headResult[0][0].(string))
		cmHash := hash.Parse(row[0].(string))

		commitsInfo = append(commitsInfo, logNode{
			commitMeta:   meta,
			commitHash:   cmHash,
			parentHashes: parents,
			branchNames:  refs,
			isHead:       cmHash == headHash})
	}

	logToStdOut(opts, commitsInfo)

	return 0
}

func tableExists(ctx context.Context, commit *doltdb.Commit, tableName string) (bool, error) {
	rv, err := commit.GetRootValue(ctx)
	if err != nil {
		return false, err
	}

	_, ok, err := rv.GetTable(ctx, tableName)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func logTableCommits(ctx context.Context, dEnv *env.DoltEnv, opts *logOpts) error {
	hashes := make([]hash.Hash, len(opts.commitSpecs))

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return err
	}

	for i, cs := range opts.commitSpecs {
		commit, err := dEnv.DoltDB.Resolve(ctx, cs, headRef)
		if err != nil {
			return err
		}

		h, err := commit.HashOf()
		if err != nil {
			return err
		}

		// Check that the table exists in the head commits
		exists, err := tableExists(ctx, commit, opts.tableName)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("error: table %s does not exist", opts.tableName)
		}

		hashes[i] = h
	}

	matchFunc := func(commit *doltdb.Commit) (bool, error) {
		return commit.NumParents() >= opts.minParents, nil
	}

	itr, err := commitwalk.GetTopologicalOrderIterator(ctx, dEnv.DoltDB, hashes, matchFunc)
	if err != nil && err != io.EOF {
		return err
	}

	var prevCommit *doltdb.Commit = nil
	var prevHash hash.Hash
	var commitsInfo []logNode

	numLines := opts.numLines
	for {
		// If we reached the limit then break
		if numLines == 0 {
			break
		}

		h, c, err := itr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if prevCommit == nil {
			prevCommit = c
			prevHash = h
			continue
		}

		parentRV, err := c.GetRootValue(ctx)
		if err != nil {
			return err
		}

		childRV, err := prevCommit.GetRootValue(ctx)
		if err != nil {
			return err
		}

		ok, err := didTableChangeBetweenRootValues(ctx, childRV, parentRV, opts.tableName)
		if err != nil {
			return err
		}

		if ok {
			meta, err := prevCommit.GetCommitMeta(ctx)
			if err != nil {
				return err
			}

			ph, err := prevCommit.ParentHashes(ctx)
			if err != nil {
				return err
			}

			commitsInfo = append(commitsInfo, logNode{
				commitMeta:   meta,
				commitHash:   prevHash,
				parentHashes: ph})

			numLines--
		}

		prevCommit = c
		prevHash = h
	}

	logToStdOut(opts, commitsInfo)

	return nil
}

func logRefs(pager *outputpager.Pager, comm logNode) {
	// Do nothing if no associate branches
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

func logCompact(pager *outputpager.Pager, opts *logOpts, commits []logNode) {
	for _, comm := range commits {
		if len(comm.parentHashes) < opts.minParents {
			return
		}

		chStr := comm.commitHash.String()
		if opts.showParents {
			for _, h := range comm.parentHashes {
				chStr += " " + h.String()
			}
		}

		// TODO: use short hash instead
		// Write commit hash
		pager.Writer.Write([]byte(fmt.Sprintf("\033[33m%s \033[0m", chStr)))

		if opts.decoration != "no" {
			logRefs(pager, comm)
		}

		formattedDesc := strings.Replace(comm.commitMeta.Description, "\n", " ", -1) + "\n"
		pager.Writer.Write([]byte(formattedDesc))
	}
}

func PrintCommit(pager *outputpager.Pager, minParents int, showParents bool, decoration string, comm logNode) {
	if len(comm.parentHashes) < minParents {
		return
	}

	chStr := comm.commitHash.String()
	if showParents {
		for _, h := range comm.parentHashes {
			chStr += " " + h.String()
		}
	}

	// Write commit hash
	pager.Writer.Write([]byte(fmt.Sprintf("\033[33mcommit %s \033[0m", chStr))) // Use Dim Yellow (33m)

	// Show decoration
	if decoration != "no" {
		logRefs(pager, comm)
	}

	if len(comm.parentHashes) > 1 {
		pager.Writer.Write([]byte("\nMerge:"))
		for _, h := range comm.parentHashes {
			pager.Writer.Write([]byte(fmt.Sprintf(" " + h.String())))
		}
	}

	pager.Writer.Write([]byte(fmt.Sprintf("\nAuthor: %s <%s>", comm.commitMeta.Name, comm.commitMeta.Email)))

	timeStr := comm.commitMeta.FormatTS()
	pager.Writer.Write([]byte(fmt.Sprintf("\nDate:  %s", timeStr)))

	formattedDesc := "\n\n\t" + strings.Replace(comm.commitMeta.Description, "\n", "\n\t", -1) + "\n\n"
	pager.Writer.Write([]byte(formattedDesc))
}

func logDefault(pager *outputpager.Pager, opts *logOpts, commits []logNode) {
	for _, comm := range commits {
		PrintCommit(pager, opts.minParents, opts.showParents, opts.decoration, comm)
	}
}

func logToStdOut(opts *logOpts, commits []logNode) {
	if cli.ExecuteWithStdioRestored == nil {
		return
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()
		if opts.oneLine {
			logCompact(pager, opts, commits)
		} else {
			logDefault(pager, opts, commits)
		}
	})
}

func didTableChangeBetweenRootValues(ctx context.Context, child, parent *doltdb.RootValue, tableName string) (bool, error) {
	childHash, ok, err := child.GetTableHash(ctx, tableName)

	if !ok {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	parentHash, ok, err := parent.GetTableHash(ctx, tableName)

	// If the table didn't exist in the parent then we know there was a change
	if !ok {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	return childHash != parentHash, nil
}

func handleErrAndExit(err error) int {
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	return 0
}
