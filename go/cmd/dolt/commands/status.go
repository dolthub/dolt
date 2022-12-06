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
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var statusDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show the working status",
	LongDesc:  `Displays working tables that differ from the current HEAD commit, tables that differ from the staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would commit by running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}; the second and third are what you could commit by running {{.EmphasisLeft}}dolt add .{{.EmphasisRight}} before running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,
	Synopsis:  []string{""},
}

type StatusCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StatusCmd) Name() string {
	return "status"
}

// Description returns a description of the command
func (cmd StatusCmd) Description() string {
	return "Show the working tree status."
}

func (cmd StatusCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(statusDocs, ap)
}

func (cmd StatusCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, statusDocs, ap))
	cli.ParseArgsOrDie(ap, args, help)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return handleStatusVErr(err)
	}

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, roots)
	if err != nil {
		return handleStatusVErr(err)
	}

	workingTblsWithViolations, _, _, err := merge.GetTablesWithConstraintViolations(ctx, roots)
	if err != nil {
		return handleStatusVErr(err)
	}

	err = PrintStatus(ctx, dEnv, staged, notStaged, workingTblsInConflict, workingTblsWithViolations)
	if err != nil {
		return handleStatusVErr(err)
	}

	return 0
}

// TODO: working docs in conflict param not used here
func PrintStatus(ctx context.Context, dEnv *env.DoltEnv, stagedTbls, notStagedTbls []diff.TableDelta, workingTblsInConflict, workingTblsWithViolations []string) error {
	cli.Printf(branchHeader, dEnv.RepoStateReader().CWBHeadRef().GetPath())

	err := printRemoteRefTrackingInfo(ctx, dEnv)
	if err != nil {
		return err
	}

	mergeActive, err := dEnv.IsMergeActive(ctx)
	if err != nil {
		return err
	}

	if mergeActive {
		if len(workingTblsInConflict) > 0 && len(workingTblsWithViolations) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts and constraint violations"))
		} else if len(workingTblsInConflict) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts"))
		} else if len(workingTblsWithViolations) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "constraint violations"))
		} else {
			cli.Println(allMergedHeader)
		}
	}

	n := printStagedDiffs(cli.CliOut, stagedTbls, true)
	n = PrintDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, true, n, workingTblsInConflict, workingTblsWithViolations)

	if !mergeActive && n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}

	return nil
}

func handleStatusVErr(err error) int {
	cli.PrintErrln(errhand.VerboseErrorFromError(err).Verbose())
	return 1
}

// printRemoteRefTrackingInfo prints remote tracking information if there is a remote branch set upstream from current branch
func printRemoteRefTrackingInfo(ctx context.Context, dEnv *env.DoltEnv) error {
	ddb := dEnv.DoltDB
	rsr := dEnv.RepoStateReader()
	headRef := rsr.CWBHeadRef()
	branches, err := rsr.GetBranches()
	if err != nil {
		return err
	}
	upstream, hasUpstream := branches[headRef.GetPath()]
	if !hasUpstream {
		return nil
	}

	// Get local head branch
	headCommitSpec, err := doltdb.NewCommitSpec(headRef.GetPath())
	if err != nil {
		return err
	}
	headCommit, err := ddb.Resolve(ctx, headCommitSpec, headRef)
	if err != nil {
		return err
	}
	headHash, err := headCommit.HashOf()
	if err != nil {
		return err
	}

	// Get remote tracking branch
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return err
	}
	remote, remoteOK := remotes[upstream.Remote]
	if !remoteOK {
		return nil
	}
	remoteTrackingRef, err := env.GetTrackingRef(upstream.Merge.Ref, remote)
	if err != nil {
		return err
	}
	remoteCommitSpec, err := doltdb.NewCommitSpec(remoteTrackingRef.GetPath())
	if err != nil {
		return err
	}
	remoteCommit, err := ddb.Resolve(ctx, remoteCommitSpec, remoteTrackingRef)
	if err != nil {
		return err
	}
	remoteHash, err := remoteCommit.HashOf()
	if err != nil {
		return err
	}

	// get common ancestor
	ancCommit, err := doltdb.GetCommitAncestor(ctx, headCommit, remoteCommit)
	if err != nil {
		return err
	}
	ancHash, err := ancCommit.HashOf()
	if err != nil {
		return err
	}

	ahead := 0
	behind := 0
	if headHash != remoteHash {
		behind, err = countCommitsInRange(ctx, ddb, remoteHash, ancHash)
		if err != nil {
			return err
		}
		ahead, err = countCommitsInRange(ctx, ddb, headHash, ancHash)
		if err != nil {
			return err
		}
	}

	cli.Println(getRemoteTrackingMsg(remoteTrackingRef.GetPath(), ahead, behind))
	return nil
}

// countCommitsInRange returns the number of commits between the given starting point to trace back to the given target point.
// The starting commit must be a descendant of the target commit. Target commit must be a common ancestor commit.
func countCommitsInRange(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash, targetCommitHash hash.Hash) (int, error) {
	itr, iErr := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{startCommitHash}, nil)
	if iErr != nil {
		return 0, iErr
	}
	count := 0
	for {
		hash, _, err := itr.Next(ctx)
		if err == io.EOF {
			return 0, errors.New("no match found to ancestor commit")
		} else if err != nil {
			return 0, err
		}

		if hash == targetCommitHash {
			break
		}
		count += 1
	}

	return count, nil
}

// getRemoteTrackingMsg returns remote tracking information with given remote branch name, number of commits ahead and/or behind.
func getRemoteTrackingMsg(remoteBranchName string, ahead int, behind int) string {
	if ahead > 0 && behind > 0 {
		return fmt.Sprintf(`Your branch and '%s' have diverged,
and have %v and %v different commits each, respectively.
  (use "dolt pull" to update your local branch)`, remoteBranchName, ahead, behind)
	} else if ahead > 0 {
		s := ""
		if ahead > 1 {
			s = "s"
		}
		return fmt.Sprintf(`Your branch is ahead of '%s' by %v commit%s.
  (use "dolt push" to publish your local commits)`, remoteBranchName, ahead, s)
	} else if behind > 0 {
		s := ""
		if behind > 1 {
			s = "s"
		}
		return fmt.Sprintf(`Your branch is behind '%s' by %v commit%s, and can be fast-forwarded.
  (use "dolt pull" to update your local branch)`, remoteBranchName, behind, s)
	} else {
		return fmt.Sprintf("Your branch is up to date with '%s'.", remoteBranchName)
	}
}
