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

package commands

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

type Test struct{}

func (t Test) Name() string {
	return "test"
}

func (t Test) Description() string {
	return "test transaction merge [writers] [connections] [values]"
}

func (t Test) Hidden() bool {
	return true
}

func (t Test) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	writers := 20
	if len(args) > 0 {
		var err error
		writers, err = strconv.Atoi(args[0])
		if err != nil {
			return 1
		}
	}

	simultaneous := 10
	if len(args) > 1 {
		var err error
		simultaneous, err = strconv.Atoi(args[1])
		if err != nil {
			return 1
		}
	}

	numValues := 10
	if len(args) > 2 {
		var err error
		numValues, err = strconv.Atoi(args[2])
		if err != nil {
			return 1
		}
	}

	work := make(chan int)

	var err error
	wg := sync.WaitGroup{}
	for i := 0; i < simultaneous; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i2 := range work {
				runnerNumber := i2
				updateErr := t.updateWorkingSet(ctx, dEnv, fmt.Sprintf("%d", runnerNumber), numValues)
				if err != nil {
					err = updateErr
				}
			}
		}()
	}

	for i := 0; i < writers; i++ {
		logmsg("sending %d\n", i)
		work <- i
	}

	close(work)

	wg.Wait()

	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("oopsie").AddCause(err).Build(), nil)
	}

	wsRef := ref.NewWorkingSetRef("test-workingset5")

	ws, err := dEnv.DoltDB.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return 1
	}

	root := ws.RootValue()
	verr := UpdateWorkingWithVErr(dEnv, root)
	return HandleVErrAndExitCode(verr, nil)
}

// Gets the current working set, alters it, then tries to commit it back
func (t Test) updateWorkingSet(ctx context.Context, dEnv *env.DoltEnv, name string, numValues int) error {

	wsRef := ref.NewWorkingSetRef("test-workingset5")

	ws, err := dEnv.DoltDB.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	origRoot := ws.RootValue()
	table, _, err := origRoot.GetTable(ctx, "test")
	if err != nil {
		return err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	tableEditor, err := editor.NewTableEditor(ctx, table, sch, "test")
	if err != nil {
		return err
	}

	for i := 0; i < numValues; i++ {
		r := make(row.TaggedValues)
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			r[tag] = types.Int(rand.Int63())
			return false, nil
		})

		toInsert, err := row.New(dEnv.DoltDB.Format(), sch, r)
		if err != nil {
			return err
		}

		err = tableEditor.InsertRow(ctx, toInsert)
		if err != nil {
			return err
		}
	}

	newTable, err := tableEditor.Table(ctx)
	if err != nil {
		return err
	}

	newRoot, err := origRoot.PutTable(ctx, "test", newTable)
	if err != nil {
		return err
	}

	// Merge newRoot into working set
	// in merge.Merger terms,
	// |root| is the ancRoot
	// |newRoot| is the mergeRoot
	// |workingSet| is root
	// if working set == ancRoot, attempt a fast-forward merge
	for i := 0; i < 100; i++ {
		ws, err := dEnv.DoltDB.ResolveWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}

		root := ws.RootValue()

		hash, err := ws.Struct().Hash(dEnv.DoltDB.Format())
		if err != nil {
			return err
		}

		if rootsEqual(root, origRoot) {
			logmsg("routine %s attempting a ff merge\n", name)
			err = dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, newRoot, hash)
			if err == datas.ErrOptimisticLockFailed {
				logmsg("routine %s failed to lock\n", name)
				continue
			}

			if err == nil {
				logmsg("routine %s committed successfully\n", name)
			}

			return err
		}

		logmsg("routine %s attempting to merge roots\n", name)
		mergedRoot, _, err := merge.MergeRoots(ctx, root, newRoot, origRoot)
		if err != nil {
			return err
		}

		err = dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, mergedRoot, hash)
		if err == datas.ErrOptimisticLockFailed {
			logmsg("routine %s failed to lock\n", name)
			continue
		}

		if err == nil {
			logmsg("routine %s committed successfully\n", name)
		}

		return err
	}

	return fmt.Errorf("Couldn't commit")
}

func (t Test) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func rootsEqual(left, right *doltdb.RootValue) bool {
	lh, err := left.HashOf()
	if err != nil {
		return false
	}

	rh, err := right.HashOf()
	if err != nil {
		return false
	}

	return lh == rh
}

func logmsg(msg string, args ...interface{}) {
	return
	cli.PrintErrf(msg, args...)
}