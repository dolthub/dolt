// Copyright 2021 Dolthub, Inc.
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

package dfunctions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

const DoltPullFuncName = "dolt_pull"

// Deprecated: please use the version in the dprocedures package
type DoltPullFunc struct {
	expression.NaryExpression
}

// NewPullFunc creates a new PullFunc expression.
// Deprecated: please use the version in the dprocedures package
func NewPullFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltPullFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltPullFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_PULL(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltPullFunc) Type() sql.Type {
	return sql.Boolean
}

func (d DoltPullFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPullFunc(children...)
}

func (d DoltPullFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return noConflictsOrViolations, err
	}
	conflicts, _, err := DoDoltPull(ctx, args)
	return conflicts, err
}

// DoDoltPull returns conflicts, fast_forward statuses
func DoDoltPull(ctx *sql.Context, args []string) (int, int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("empty database name.")
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

	if apr.NArg() > 1 {
		return noConflictsOrViolations, threeWayMerge, actions.ErrInvalidPullArgs
	}

	var remoteName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	}

	pullSpec, err := env.NewPullSpec(ctx, dbData.Rsr, remoteName, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag))
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	srcDB, err := pullSpec.Remote.GetRemoteDBWithoutCaching(ctx, dbData.Ddb.ValueReadWriter().Format())
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("failed to get remote db; %w", err)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	var conflicts int
	var fastForward int
	for _, refSpec := range pullSpec.RefSpecs {
		remoteTrackRef := refSpec.DestRef(pullSpec.Branch)

		if remoteTrackRef != nil {

			// todo: can we pass nil for either of the channels?
			srcDBCommit, err := actions.FetchRemoteBranch(ctx, dbData.Rsw.TempTableFilesDir(), pullSpec.Remote, srcDB, dbData.Ddb, pullSpec.Branch, runProgFuncs, stopProgFuncs)
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, err
			}

			// TODO: this could be replaced with a canFF check to test for error
			err = dbData.Ddb.FastForward(ctx, remoteTrackRef, srcDBCommit)
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, fmt.Errorf("fetch failed; %w", err)
			}

			roots, ok := sess.GetRoots(ctx, dbName)
			if !ok {
				return noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
			}

			mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, remoteTrackRef.String())
			if err != nil {
				return noConflictsOrViolations, threeWayMerge, err
			}
			ws, conflicts, fastForward, err = mergeIntoWorkingSet(ctx, sess, roots, ws, dbName, mergeSpec)
			if err != nil && !errors.Is(doltdb.ErrUpToDate, err) {
				return conflicts, fastForward, err
			}

			err = sess.SetWorkingSet(ctx, dbName, ws)
			if err != nil {
				return conflicts, fastForward, err
			}
		}
	}

	err = actions.FetchFollowTags(ctx, dbData.Rsw.TempTableFilesDir(), srcDB, dbData.Ddb, runProgFuncs, stopProgFuncs)
	if err != nil {
		return conflicts, fastForward, err
	}

	return conflicts, fastForward, nil
}

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

func progFunc(ctx context.Context, progChan <-chan pull.PullProgress) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-progChan:
		default:
		}
	}
}

func runProgFuncs(ctx context.Context) (*sync.WaitGroup, chan pull.PullProgress, chan pull.Stats) {
	statsCh := make(chan pull.Stats)
	progChan := make(chan pull.PullProgress)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(ctx, progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(ctx, statsCh)
	}()

	return wg, progChan, statsCh
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan pull.PullProgress, statsCh chan pull.Stats) {
	cancel()
	close(progChan)
	close(statsCh)
	wg.Wait()
}
