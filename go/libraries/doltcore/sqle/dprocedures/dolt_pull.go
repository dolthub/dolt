// Copyright 2022 Dolthub, Inc.
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

package dprocedures

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

// For callers of dolt_pull(), the index of the FastForward column is needed to print results. If the schema of
// the result changes, this will need to be updated.
const PullProcFFIndex = 0

var doltPullSchema = []*sql.Column{
	{
		Name:     "fast_forward",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "message",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
}

// doltPull is the stored procedure version for the CLI command `dolt pull`.
func doltPull(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	conflicts, ff, msg, err := doDoltPull(ctx, args)
	if err != nil {
		return nil, err
	}

	if msg == "" {
		return rowToIter(int64(ff), int64(conflicts), nil), nil
	}
	return rowToIter(int64(ff), int64(conflicts), msg), nil
}

// doDoltPull returns conflicts, fast_forward statuses
func doDoltPull(ctx *sql.Context, args []string) (int, int, string, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return noConflictsOrViolations, threeWayMerge, "", sql.ErrDatabaseNotFound.New(dbName)
	}

	apr, err := cli.CreatePullArgParser().Parse(args)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}

	if apr.NArg() > 2 {
		return noConflictsOrViolations, threeWayMerge, "", actions.ErrInvalidPullArgs
	}

	// Validate conflicting flags
	if apr.ContainsAll(cli.FFOnlyParam, cli.NoFFParam) {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.FFOnlyParam, cli.NoFFParam)
	}
	if apr.ContainsAll(cli.FFOnlyParam, cli.SquashParam) {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.FFOnlyParam, cli.SquashParam)
	}
	if apr.Contains(cli.RebaseParam) {
		if apr.Contains(cli.SquashParam) {
			return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.RebaseParam, cli.SquashParam)
		}
		if apr.Contains(cli.NoFFParam) {
			return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.RebaseParam, cli.NoFFParam)
		}
		if apr.Contains(cli.FFOnlyParam) {
			return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.RebaseParam, cli.FFOnlyParam)
		}
		if apr.Contains(cli.NoCommitFlag) {
			return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together", cli.RebaseParam, cli.NoCommitFlag)
		}
	}

	var remoteName, remoteRefName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	} else if apr.NArg() == 2 {
		remoteName = apr.Arg(0)
		remoteRefName = apr.Arg(1)
	}

	remoteOnly := apr.NArg() == 1
	pullSpec, err := env.NewPullSpec(
		ctx,
		dbData.Rsr,
		remoteName,
		remoteRefName,
		remoteOnly,
		env.WithSquash(apr.Contains(cli.SquashParam)),
		env.WithNoFF(apr.Contains(cli.NoFFParam)),
		env.WithNoCommit(apr.Contains(cli.NoCommitFlag)),
		env.WithNoEdit(apr.Contains(cli.NoEditFlag)),
		env.WithForce(apr.Contains(cli.ForceFlag)),
	)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}

	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		pullSpec.Remote = pullSpec.Remote.WithParams(map[string]string{
			dbfactory.GRPCUsernameAuthParam: user,
		})
	}

	srcDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), pullSpec.Remote, false)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("failed to get remote db; %w", err)
	}

	err = srcDB.Rebase(ctx)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("failed to read latest version of remote db: %w", err)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}

	// Assert the branch exists
	_, hasBranch, err := srcDB.HasBranch(ctx, pullSpec.Branch.GetPath())
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}
	if !hasBranch {
		return noConflictsOrViolations, threeWayMerge, "",
			fmt.Errorf("branch %q not found on remote", pullSpec.Branch.GetPath())
	}

	// Fetch all references
	branchRefs, err := srcDB.GetHeadRefs(ctx)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("%w: %s", env.ErrFailedToReadDb, err.Error())
	}
	prune := apr.Contains(cli.PruneFlag)
	mode := ref.UpdateMode{Force: true, Prune: prune}
	pull.WithDiscardingStatsCh(func(statsCh chan pull.Stats) {
		err = actions.FetchRefSpecs(ctx, dbData, srcDB, pullSpec.RefSpecs, false, &pullSpec.Remote, mode, statsCh)
	})
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("fetch failed: %w", err)
	}

	var conflicts int
	var fastForward int
	var message string
	for _, refSpec := range pullSpec.RefSpecs {
		rsSeen := false // track invalid refSpecs
		for _, branchRef := range branchRefs {
			remoteTrackRef := refSpec.DestRef(branchRef)

			if remoteTrackRef == nil {
				continue
			}

			if branchRef != pullSpec.Branch {
				continue
			}

			rsSeen = true

			headRef, err := dbData.Rsr.CWBHeadRef(ctx)
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, "", err
			}

			if apr.Contains(cli.RebaseParam) {
				// Rebase path: use CanFastForwardTo to determine the right action
				mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, remoteTrackRef.String())
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				canFF, ffErr := mergeSpec.HeadC.CanFastForwardTo(ctx, mergeSpec.MergeC)
				if ffErr != nil {
					if errors.Is(ffErr, doltdb.ErrUpToDate) || errors.Is(ffErr, doltdb.ErrIsAhead) {
						return noConflictsOrViolations, threeWayMerge, doltdb.ErrUpToDate.Error(), nil
					}
					return noConflictsOrViolations, threeWayMerge, "", ffErr
				}

				if canFF {
					// No local commits to rebase; fast-forward
					ws, err = executeFFMerge(ctx, dbName, false, ws, dbData, mergeSpec.MergeC, mergeSpec)
					if err != nil {
						return noConflictsOrViolations, fastForwardMerge, "", err
					}
					return noConflictsOrViolations, fastForwardMerge, "merge successful", nil
				}

				// Diverged: rebase local commits onto remote tip
				conflicts, fastForward, message, err = performRebaseAfterPull(ctx, remoteTrackRef.String(), apr.Contains(cli.SkipVerificationFlag))
				if err != nil {
					return conflicts, fastForward, message, err
				}
			} else {
				// Merge path
				msg := fmt.Sprintf("Merge branch '%s' of %s into %s", pullSpec.Branch.GetPath(), pullSpec.Remote.Url, headRef.GetPath())

				roots, ok := sess.GetRoots(ctx, dbName)
				if !ok {
					return noConflictsOrViolations, threeWayMerge, "", sql.ErrDatabaseNotFound.New(dbName)
				}

				mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, remoteTrackRef.String())
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				roots, err = actions.ClearFeatureVersion(context.Background(), roots)
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				headHash, err := roots.Head.HashOf()
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				stagedHash, err := roots.Staged.HashOf()
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				if headHash != stagedHash {
					return noConflictsOrViolations, threeWayMerge, "", ErrUncommittedChanges.New()
				}

				// We allow changes to ignored tables. If this causes a conflict because the remote also modified these tables,
				// we will detect that during the pull.
				workingSetClean, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
				if err != nil {
					return noConflictsOrViolations, threeWayMerge, "", err
				}

				if !workingSetClean {
					return noConflictsOrViolations, threeWayMerge, "", ErrUncommittedChanges.New()
				}

				ws, _, conflicts, fastForward, message, err = performMerge(ctx, sess, ws, dbName, mergeSpec, apr.Contains(cli.NoCommitFlag), msg, apr.Contains(cli.SkipVerificationFlag))
				if err != nil && !errors.Is(doltdb.ErrUpToDate, err) {
					return conflicts, fastForward, "", err
				}
			}
		}
		if !rsSeen {
			return noConflictsOrViolations, threeWayMerge, "", fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, refSpec.GetRemRefToLocal())
		}
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}
	pull.WithDiscardingStatsCh(func(statsCh chan pull.Stats) {
		err = actions.FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, statsCh)
	})
	if err != nil {
		return conflicts, fastForward, "", err
	}

	return conflicts, fastForward, message, nil
}

// performRebaseAfterPull runs a non-interactive rebase of the current branch onto
// the given upstream point. This is used by dolt_pull when --rebase is specified.
func performRebaseAfterPull(ctx *sql.Context, upstreamPoint string, skipVerification bool) (int, int, string, error) {
	commitBecomesEmptyHandling := doltdb.EmptyCommitHandling(doltdb.DropEmptyCommit)
	emptyCommitHandling := doltdb.EmptyCommitHandling(doltdb.KeepEmptyCommit)

	err := startRebase(ctx, upstreamPoint, commitBecomesEmptyHandling, emptyCommitHandling, skipVerification)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, "", err
	}

	result := continueRebase(ctx)
	if result.err != nil {
		return hasConflictsOrViolations, threeWayMerge, "", result.err
	}

	return noConflictsOrViolations, threeWayMerge, result.message, nil
}
