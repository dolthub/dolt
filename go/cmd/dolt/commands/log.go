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
	"strings"

	"github.com/fatih/color"

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

const (
	numLinesParam   = "number"
	mergesParam     = "merges"
	minParentsParam = "min-parents"
	parentsParam    = "parents"
	decorateParam   = "decorate"
	oneLineParam    = "oneline"
	notParam        = "not"
)

type logOpts struct {
	numLines            int
	showParents         bool
	minParents          int
	decoration          string
	oneLine             bool
	excludingCommitSpec *doltdb.CommitSpec
	commitSpec          *doltdb.CommitSpec
	tableName           string
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
	
{{.EmphasisLeft}}dolt log <revision>{{.EmphasisRight}}
  Lists commit logs starting from revision.
	
{{.EmphasisLeft}}dolt log <revision> <table>{{.EmphasisRight}}
  Lists commit logs starting from revision, only including commits with changes to table.
	
{{.EmphasisLeft}}dolt log <revisionB>..<revisionA>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log <revisionA> --not <revisionB>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log ^<revisionB> <revisionA>{{.EmphasisRight}}
  Different ways to list two dot logs. These will list commit logs for revisionA, while excluding commits from revisionB. The table option is not supported for two dot log.`,
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
	ap := argparser.NewArgParser()
	ap.SupportsInt(numLinesParam, "n", "num_commits", "Limit the number of commits to output.")
	ap.SupportsInt(minParentsParam, "", "parent_count", "The minimum number of parents a commit must have to be included in the log.")
	ap.SupportsFlag(mergesParam, "", "Equivalent to min-parents == 2, this will limit the log to commits with 2 or more parents.")
	ap.SupportsFlag(parentsParam, "", "Shows all parents of each commit in the log.")
	ap.SupportsString(decorateParam, "", "decorate_fmt", "Shows refs next to commits. Valid options are short, full, no, and auto")
	ap.SupportsFlag(oneLineParam, "", "Shows logs in a compact format.")
	ap.SupportsString(notParam, "", "revision", "Excludes commits from revision.")
	return ap
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, logDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 2 {
		usage()
		return 1
	}

	opts, err := parseLogArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if opts.commitSpec == nil {
		opts.commitSpec = dEnv.RepoStateReader().CWBHeadSpec()
	}
	if len(opts.tableName) > 0 {
		return handleErrAndExit(logTableCommits(ctx, dEnv, opts))
	}
	return logCommits(ctx, dEnv, opts)
}

func parseLogArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*logOpts, error) {
	minParents := apr.GetIntOrDefault(minParentsParam, 0)
	if apr.Contains(mergesParam) {
		minParents = 2
	}

	decorateOption := apr.GetValueOrDefault(decorateParam, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return nil, fmt.Errorf("fatal: invalid --decorate option: %s", decorateOption)
	}

	opts := &logOpts{
		numLines:    apr.GetIntOrDefault(numLinesParam, -1),
		showParents: apr.Contains(parentsParam),
		minParents:  minParents,
		oneLine:     apr.Contains(oneLineParam),
		decoration:  decorateOption,
	}
	cs, notCs, tableName, err := parseRefsAndTable(ctx, apr, dEnv)
	if err != nil {
		return nil, err
	}
	opts.commitSpec = cs
	opts.excludingCommitSpec = notCs
	opts.tableName = tableName

	excludingRef, ok := apr.GetValue(notParam)
	if ok {
		if opts.excludingCommitSpec != nil {
			return nil, fmt.Errorf("cannot use --not argument with two dots or ref with ^")
		}
		if len(opts.tableName) > 0 {
			return nil, fmt.Errorf("cannot use --not argument with table")
		}
		cs, err := doltdb.NewCommitSpec(excludingRef)
		if err != nil {
			return nil, fmt.Errorf("invalid commit %s\n", excludingRef)
		}
		opts.excludingCommitSpec = cs
	}

	return opts, nil
}

func parseRefsAndTable(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) (*doltdb.CommitSpec, *doltdb.CommitSpec, string, error) {
	switch apr.NArg() {
	// dolt log
	case 0:
		return nil, nil, "", nil

	// dolt log <ref/^ref/revision-range/table>
	case 1:
		return getCommitOrTableFromString(ctx, apr.Arg(0), dEnv, true)

	// dolt log <ref/^ref> <ref/^ref/table>
	case 2:
		firstCs, firstExNotCs, _, err := getCommitOrTableFromString(ctx, apr.Arg(0), dEnv, false)
		if err != nil {
			return nil, nil, "", err
		}

		secondCs, secondExNotCs, tableName, err := getCommitOrTableFromString(ctx, apr.Arg(1), dEnv, false)
		if err != nil {
			return nil, nil, "", err
		}

		if len(tableName) > 0 {
			if firstExNotCs != nil {
				return nil, nil, "", fmt.Errorf("Providing tableName for two dot log not yet supported")
			}
			// dolt log <ref> <table>
			return firstCs, nil, tableName, nil
		}

		if firstCs != nil && secondCs != nil {
			commit, err := dEnv.DoltDB.Resolve(ctx, firstCs, dEnv.RepoStateReader().CWBHeadRef())
			if err != nil {
				return nil, nil, "", err
			}

			// Handles table name matching branch name (dolt log <ref> <table>)
			exists, err := tableExists(ctx, commit, apr.Arg(1))
			if err != nil {
				return nil, nil, "", err
			}

			if exists {
				return firstCs, nil, apr.Arg(1), nil
			}

			return nil, nil, "", fmt.Errorf("Cannot provide two commit refs") // dolt log <ref> <ref>
		}
		if firstExNotCs != nil && secondExNotCs != nil {
			return nil, nil, "", fmt.Errorf("Cannot exclude two commit refs") // dolt log ^<ref> ^<ref>
		}
		// dolt log ^<ref> <ref>
		if firstExNotCs != nil && secondCs != nil {
			return secondCs, firstExNotCs, "", nil
		}
		// dolt log <ref> ^<ref>
		if firstCs != nil && secondExNotCs != nil {
			return firstCs, secondExNotCs, "", nil
		}

		return nil, nil, "", nil

	default:
		return nil, nil, "", fmt.Errorf("Cannot provide more than 3 arguments")
	}
}

func getCommitOrTableFromString(ctx context.Context, str string, dEnv *env.DoltEnv, canDot bool) (*doltdb.CommitSpec, *doltdb.CommitSpec, string, error) {
	// <ref>...<ref>
	if strings.Contains(str, "...") {
		return nil, nil, "", fmt.Errorf("Three dot dolt log not supported")
	}

	// <ref>..<ref>
	if strings.Contains(str, "..") {
		if !canDot {
			return nil, nil, "", fmt.Errorf("Cannot use two dot when 2 arguments provided")
		}
		refs := strings.Split(str, "..")
		notCs, err := getCommitSpec(refs[0])
		if err != nil {
			return nil, nil, "", err
		}

		cs, err := getCommitSpec(refs[1])
		if err != nil {
			return nil, nil, "", err
		}

		return cs, notCs, "", nil
	}

	// ^<ref>
	if strings.Contains(str, "^") {
		commit := strings.TrimPrefix(str, "^")
		notCs, err := getCommitSpec(commit)
		if err != nil {
			return nil, nil, "", err
		}
		return nil, notCs, "", err
	}

	argIsRef := actions.ValidateIsRef(ctx, str, dEnv.DoltDB, dEnv.RepoStateReader())
	// <ref>
	if argIsRef {
		cs, err := getCommitSpec(str)
		if err != nil {
			return nil, nil, "", err
		}
		return cs, nil, "", nil
	}

	// <table>
	return nil, nil, str, nil
}

func getCommitSpec(commit string) (*doltdb.CommitSpec, error) {
	cs, err := doltdb.NewCommitSpec(commit)
	if err != nil {
		return nil, fmt.Errorf("invalid commit %s\n", commit)
	}
	return cs, nil
}

func logCommits(ctx context.Context, dEnv *env.DoltEnv, opts *logOpts) int {
	commit, err := dEnv.DoltDB.Resolve(ctx, opts.commitSpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	cHashToRefs := map[hash.Hash][]string{}

	// Get all branches
	branches, err := dEnv.DoltDB.GetBranchesWithHashes(ctx)
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get Branch information."))
		return 1
	}
	for _, b := range branches {
		refName := b.Ref.String()
		if opts.decoration != "full" {
			refName = b.Ref.GetPath() // trim out "refs/heads/"
		}
		refName = fmt.Sprintf("\033[32;1m%s\033[0m", refName) // branch names are bright green (32;1m)
		cHashToRefs[b.Hash] = append(cHashToRefs[b.Hash], refName)
	}

	// Get all remote branches
	remotes, err := dEnv.DoltDB.GetRemotesWithHashes(ctx)
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get Branch information."))
		return 1
	}
	for _, r := range remotes {
		refName := r.Ref.String()
		if opts.decoration != "full" {
			refName = r.Ref.GetPath() // trim out "refs/remotes/"
		}
		refName = fmt.Sprintf("\033[31;1m%s\033[0m", refName) // remote names are bright red (31;1m)
		cHashToRefs[r.Hash] = append(cHashToRefs[r.Hash], refName)
	}

	// Get all tags
	tags, err := dEnv.DoltDB.GetTagsWithHashes(ctx)
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get Tag information."))
		return 1
	}
	for _, t := range tags {
		tagName := t.Tag.GetDoltRef().String()
		if opts.decoration != "full" {
			tagName = t.Tag.Name // trim out "refs/tags/"
		}
		tagName = fmt.Sprintf("\033[33;1mtag: %s\033[0m", tagName) // tags names are bright yellow (33;1m)
		cHashToRefs[t.Hash] = append(cHashToRefs[t.Hash], tagName)
	}

	h, err := commit.HashOf()

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
		return 1
	}

	matchFunc := func(commit *doltdb.Commit) (bool, error) {
		return commit.NumParents() >= opts.minParents, nil
	}

	var commits []*doltdb.Commit
	if opts.excludingCommitSpec == nil {
		commits, err = commitwalk.GetTopNTopoOrderedCommitsMatching(ctx, dEnv.DoltDB, h, opts.numLines, matchFunc)
	} else {
		excludingCommit, err := dEnv.DoltDB.Resolve(ctx, opts.excludingCommitSpec, dEnv.RepoStateReader().CWBHeadRef())
		if err != nil {
			cli.PrintErrln(color.HiRedString("Fatal error: cannot get excluding commit for current branch."))
			return 1
		}

		excludingHash, err := excludingCommit.HashOf()

		if err != nil {
			cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
			return 1
		}

		commits, err = commitwalk.GetDotDotRevisions(ctx, dEnv.DoltDB, h, dEnv.DoltDB, excludingHash, opts.numLines)
	}

	if err != nil {
		cli.PrintErrln("Error retrieving commit.")
		return 1
	}

	var commitsInfo []logNode
	for _, comm := range commits {
		meta, mErr := comm.GetCommitMeta(ctx)
		if mErr != nil {
			cli.PrintErrln("error: failed to get commit metadata")
			return 1
		}

		pHashes, pErr := comm.ParentHashes(ctx)
		if pErr != nil {
			cli.PrintErrln("error: failed to get parent hashes")
			return 1
		}

		cmHash, cErr := comm.HashOf()
		if cErr != nil {
			cli.PrintErrln("error: failed to get commit hash")
			return 1
		}

		commitsInfo = append(commitsInfo, logNode{
			commitMeta:   meta,
			commitHash:   cmHash,
			parentHashes: pHashes,
			branchNames:  cHashToRefs[cmHash],
			isHead:       cmHash == h})
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
	commit, err := dEnv.DoltDB.Resolve(ctx, opts.commitSpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return err
	}

	// Check that the table exists in the head commit
	exists, err := tableExists(ctx, commit, opts.tableName)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("error: table %s does not exist", opts.tableName)
	}

	h, err := commit.HashOf()
	if err != nil {
		return err
	}

	itr, err := commitwalk.GetTopologicalOrderIterator(ctx, dEnv.DoltDB, h)
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
		pager.Writer.Write([]byte(fmt.Sprintf(formattedDesc)))
	}
}

func logDefault(pager *outputpager.Pager, opts *logOpts, commits []logNode) {
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

		// Write commit hash
		pager.Writer.Write([]byte(fmt.Sprintf("\033[33mcommit %s \033[0m", chStr))) // Use Dim Yellow (33m)

		// Show decoration
		if opts.decoration != "no" {
			logRefs(pager, comm)
		}

		if len(comm.parentHashes) > 1 {
			pager.Writer.Write([]byte(fmt.Sprintf("\nMerge:")))
			for _, h := range comm.parentHashes {
				pager.Writer.Write([]byte(fmt.Sprintf(" " + h.String())))
			}
		}

		pager.Writer.Write([]byte(fmt.Sprintf("\nAuthor: %s <%s>", comm.commitMeta.Name, comm.commitMeta.Email)))

		timeStr := comm.commitMeta.FormatTS()
		pager.Writer.Write([]byte(fmt.Sprintf("\nDate:  %s", timeStr)))

		formattedDesc := "\n\n\t" + strings.Replace(comm.commitMeta.Description, "\n", "\n\t", -1) + "\n\n"
		pager.Writer.Write([]byte(fmt.Sprintf(formattedDesc)))
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
