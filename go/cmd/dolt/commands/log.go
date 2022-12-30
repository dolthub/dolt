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
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, logDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	opts, err := parseLogArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if len(opts.commitSpecs) == 0 {
		opts.commitSpecs = append(opts.commitSpecs, dEnv.RepoStateReader().CWBHeadSpec())
	}
	if len(opts.tableName) > 0 {
		return handleErrAndExit(logTableCommits(ctx, dEnv, opts))
	}
	return logCommits(ctx, dEnv, opts)
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
			argIsRef := actions.ValidateIsRef(ctx, arg, dEnv.DoltDB, dEnv.RepoStateReader())
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

func logCommits(ctx context.Context, dEnv *env.DoltEnv, opts *logOpts) int {
	hashes := make([]hash.Hash, len(opts.commitSpecs))

	for i, cs := range opts.commitSpecs {
		commit, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())
		if err != nil {
			cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
			return 1
		}

		h, err := commit.HashOf()
		if err != nil {
			cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
			return 1
		}

		hashes[i] = h
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
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get Remotes information."))
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

	matchFunc := func(c *doltdb.Commit) (bool, error) {
		return c.NumParents() >= opts.minParents, nil
	}

	var commits []*doltdb.Commit
	if len(opts.excludingCommitSpecs) == 0 {
		commits, err = commitwalk.GetTopNTopoOrderedCommitsMatching(ctx, dEnv.DoltDB, hashes, opts.numLines, matchFunc)
	} else {
		excludingHashes := make([]hash.Hash, len(opts.excludingCommitSpecs))

		for i, excludingSpec := range opts.excludingCommitSpecs {
			excludingCommit, err := dEnv.DoltDB.Resolve(ctx, excludingSpec, dEnv.RepoStateReader().CWBHeadRef())
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
			isHead:       hashIsHead(cmHash, hashes)})
	}

	logToStdOut(opts, commitsInfo)

	return 0
}

func hashIsHead(cmHash hash.Hash, hashes []hash.Hash) bool {
	if len(hashes) > 1 || len(hashes) == 0 {
		return false
	}
	return cmHash == hashes[0]
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

	for i, cs := range opts.commitSpecs {
		commit, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())
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
		pager.Writer.Write([]byte(fmt.Sprintf("%s", formattedDesc)))
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
		pager.Writer.Write([]byte(fmt.Sprintf("%s", formattedDesc)))
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
