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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

const DoltPullFuncName = "dolt_pull"

type DoltPullFunc struct {
	expression.NaryExpression
}

// NewPullFunc creates a new MergeFunc expression.
func NewPullFunc(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

func (d DoltPullFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewPullFunc(ctx, children...)
}

func (d DoltPullFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflicts, fmt.Errorf("empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)

	if !ok {
		return noConflicts, fmt.Errorf("Could not load database %s", dbName)
	}

	ap := cli.CreateMergeArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	apr, err := ap.Parse(args)
	if err != nil {
		return noConflicts, err
	}

	if apr.NArg() > 1 {
		return nil, actions.ErrInvalidPullArgs
	}

	var remoteName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	}

	pullSpec, err := env.ParsePullSpec(ctx, dbData.Rsr, remoteName, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag))
	if err != nil {
		return nil, err
	}

	srcDB, err := pullSpec.Remote.GetRemoteDBWithoutCaching(ctx, dbData.Ddb.ValueReadWriter().Format())
	if err != nil {
		return 1, fmt.Errorf("failed to get remote db; %w", err)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return 1, err
	}

	var conflicts interface{}
	for _, refSpec := range pullSpec.RefSpecs {
		remoteTrackRef := refSpec.DestRef(pullSpec.Branch)

		if remoteTrackRef != nil {

			srcDBCommit, err := actions.FetchRemoteBranch(ctx, dbData.Rsw.TempTableFilesDir(), pullSpec.Remote, srcDB, dbData.Ddb, pullSpec.Branch, remoteTrackRef, runProgFuncs, stopProgFuncs)
			if err != nil {
				return 1, err
			}

			// TODO: I don't think this is necessary, but other merges do it
			err = dbData.Ddb.FastForward(ctx, remoteTrackRef, srcDBCommit)
			if err != nil {
				return 1, fmt.Errorf("fetch failed; %w", err)
			}

			roots, ok := sess.GetRoots(ctx, dbName)
			if !ok {
				return noConflicts, sql.ErrDatabaseNotFound.New(dbName)
			}

			mergeSpec, err := sqlMergeSpec(ctx, sess, dbName, apr, remoteTrackRef.String())
			if err != nil {
				return 1, err
			}
			ws, conflicts, err = mergeHelper(ctx, sess, roots, ws, dbName, mergeSpec)
			if !errors.Is(datas.ErrDBUpToDate, err) {
				return conflicts, err
			}
		}
	}
	err = actions.FetchFollowTags(ctx, dbData.Rsw.TempTableFilesDir(), srcDB, dbData.Ddb, runProgFuncs, stopProgFuncs)

	return 0, nil
}

func pullerProgFunc(pullerEventCh chan datas.PullerEvent) {
	//var pos int
	var currentTreeLevel int
	//var percentBuffered float64
	var tableFilesBuffered int
	var filesUploaded int

	for evt := range pullerEventCh {
		switch evt.EventType {
		case datas.NewLevelTWEvent:
			if evt.TWEventDetails.TreeLevel != 1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
				//percentBuffered = 0
			}
		case datas.DestDBHasTWEvent:
			if evt.TWEventDetails.TreeLevel != -1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
			}

		case datas.LevelUpdateTWEvent:
			if evt.TWEventDetails.TreeLevel != -1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
				toBuffer := evt.TWEventDetails.ChunksInLevel - evt.TWEventDetails.ChunksAlreadyHad

				if toBuffer > 0 {
					//percentBuffered = 100 * float64(evt.TWEventDetails.ChunksBuffered) / float64(toBuffer)
				}
			}

		case datas.LevelDoneTWEvent:

		case datas.TableFileClosedEvent:
			tableFilesBuffered += 1

		case datas.StartUploadTableFileEvent:

		case datas.UploadTableFileUpdateEvent:

		case datas.EndUploadTableFileEvent:
			filesUploaded += 1
		}

		if currentTreeLevel == -1 {
			continue
		}

		//var msg string
		//if len(uploadRate) > 0 {
		//	msg = fmt.Sprintf("%s Tree Level: %d, Percent Buffered: %.2f%%, Files Written: %d, Files Uploaded: %d, Current Upload Speed: %s", ts.next(), currentTreeLevel, percentBuffered, tableFilesBuffered, filesUploaded, uploadRate)
		//} else {
		//	msg = fmt.Sprintf("%s Tree Level: %d, Percent Buffered: %.2f%% Files Written: %d, Files Uploaded: %d", ts.next(), currentTreeLevel, percentBuffered, tableFilesBuffered, filesUploaded)
		//}
		//
		//pos = cli.DeleteAndPrint(pos, msg)
	}
}

func progFunc(progChan chan datas.PullProgress) {
	//var latest datas.PullProgress
	//last := time.Now().UnixNano() - 1
	//lenPrinted := 0
	done := false
	for !done {
		select {
		case _, ok := <-progChan:
			if !ok {
				done = true
			}
			//latest = progress

		case <-time.After(250 * time.Millisecond):
			break
		}
	}

	//if lenPrinted > 0 {
	//	cli.Println()
	//}
}

func runProgFuncs() (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent) {
	pullerEventCh := make(chan datas.PullerEvent, 128)
	progChan := make(chan datas.PullProgress, 128)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(pullerEventCh)
	}()

	return wg, progChan, pullerEventCh
}

func stopProgFuncs(wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) {
	close(progChan)
	close(pullerEventCh)
	wg.Wait()
}
