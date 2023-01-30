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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

// doltPull is the stored procedure version for the CLI command `dolt pull`.
func doltPull(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	conflicts, ff, err := doDoltPull(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(ff), int64(conflicts)), nil
}

// doDoltPull returns conflicts, fast_forward statuses
func doDoltPull(ctx *sql.Context, args []string) (int, int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
	}

	apr, err := cli.CreatePullArgParser().Parse(args)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	if apr.NArg() > 2 {
		return noConflictsOrViolations, threeWayMerge, actions.ErrInvalidPullArgs
	}

	var remoteName, remoteRefName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	} else if apr.NArg() == 2 {
		remoteName = apr.Arg(0)
		remoteRefName = apr.Arg(1)
	}

	pullSpec, err := env.NewPullSpec(ctx, dbData.Rsr, remoteName, remoteRefName, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.NoCommitFlag), apr.Contains(cli.NoEditFlag), apr.Contains(cli.ForceFlag), apr.NArg() == 1)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	srcDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), pullSpec.Remote, false)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("failed to get remote db; %w", err)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	// Fetch all references
	branchRefs, err := srcDB.GetHeadRefs(ctx)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("%w: %s", env.ErrFailedToReadDb, err.Error())
	}

	_, hasBranch, err := srcDB.HasBranch(ctx, pullSpec.Branch.GetPath())
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}
	if !hasBranch {
		return noConflictsOrViolations, threeWayMerge,
			fmt.Errorf("branch %q not found on remote", pullSpec.Branch.GetPath())
	}

	var conflicts int
	var fastForward int
	for _, refSpec := range pullSpec.RefSpecs {
		rsSeen := false // track invalid refSpecs
		for _, branchRef := range branchRefs {
			remoteTrackRef := refSpec.DestRef(branchRef)

			if remoteTrackRef == nil {
				continue
			}

			rsSeen = true
			tmpDir, err := dbData.Rsw.TempTableFilesDir()
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, err
			}
			// todo: can we pass nil for either of the channels?
			srcDBCommit, err := actions.FetchRemoteBranch(ctx, tmpDir, pullSpec.Remote, srcDB, dbData.Ddb, branchRef, runProgFuncs, stopProgFuncs)
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, err
			}

			// TODO: this could be replaced with a canFF check to test for error
			err = dbData.Ddb.FastForward(ctx, remoteTrackRef, srcDBCommit)
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, fmt.Errorf("fetch failed; %w", err)
			}

			// Only merge iff branch is current branch and there is an upstream set (pullSpec.Branch is set to nil if there is no upstream)
			if branchRef != pullSpec.Branch {
				continue
			}

			roots, ok := sess.GetRoots(ctx, dbName)
			if !ok {
				return noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
			}

			mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, remoteTrackRef.String())
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, err
			}

			msg := fmt.Sprintf("Merge branch '%s' of %s into %s", pullSpec.Branch.GetPath(), pullSpec.Remote.Url, dbData.Rsr.CWBHeadRef().GetPath())
			ws, conflicts, fastForward, err = performMerge(ctx, sess, roots, ws, dbName, mergeSpec, apr.Contains(cli.NoCommitFlag), msg)
			if err != nil && !errors.Is(doltdb.ErrUpToDate, err) {
				return conflicts, fastForward, err
			}

			err = sess.SetWorkingSet(ctx, dbName, ws)
			if err != nil {
				return conflicts, fastForward, err
			}
		}
		if !rsSeen {
			return noConflictsOrViolations, threeWayMerge, fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, refSpec.GetRemRefToLocal())
		}
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}
	err = actions.FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, runProgFuncs, stopProgFuncs)
	if err != nil {
		return conflicts, fastForward, err
	}

	return conflicts, fastForward, nil
}

// TODO: remove this as it does not do anything useful
func pullerProgFunc(ctx context.Context, statsCh <-chan pull.Stats) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-statsCh:
		default:
		}
	}
}

// TODO: remove this as it does not do anything useful
func runProgFuncs(ctx context.Context) (*sync.WaitGroup, chan pull.Stats) {
	statsCh := make(chan pull.Stats)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(ctx, statsCh)
	}()

	return wg, statsCh
}

// TODO: remove this as it does not do anything useful
func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, statsCh chan pull.Stats) {
	cancel()
	close(statsCh)
	wg.Wait()
}
