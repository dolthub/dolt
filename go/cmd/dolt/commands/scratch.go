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
	"sync"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
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
	panic("test command")
}

func (t Test) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	var err error
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		name := i
		go func() {
			defer wg.Done()
			updateErr := t.updateWorkingSet(ctx, dEnv, fmt.Sprintf("%d", name))
			if err != nil {
				err = updateErr
			}
		}()
	}

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
func (t Test) updateWorkingSet(ctx context.Context, dEnv *env.DoltEnv, name string) error {

	for i := 0; i < 100; i++ {
		wsRef := ref.NewWorkingSetRef("test-workingset5")

		ws, err := dEnv.DoltDB.ResolveWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}

		root := ws.RootValue()
		table, _, err := root.GetTable(ctx, "test")
		if err != nil {
			return err
		}

		schema, err := table.GetSchema(ctx)
		if err != nil {
			return err
		}

		tableEditor, err := editor.NewTableEditor(ctx, table, schema, "test")
		if err != nil {
			return err
		}

		for i := 0; i < 10; i++ {
			r, err := row.New(dEnv.DoltDB.Format(), schema, row.TaggedValues{7493: types.Int(rand.Int63())})
			if err != nil {
				return err
			}

			err = tableEditor.InsertRow(ctx, r)
			if err != nil {
				return err
			}
		}

		newTable, err := tableEditor.Table(ctx)
		if err != nil {
			return err
		}

		newRoot, err := root.PutTable(ctx, "test", newTable)
		if err != nil {
			return err
		}

		hash, err := ws.Struct().Hash(dEnv.DoltDB.Format())
		if err != nil {
			return err
		}

		err = dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, newRoot, hash)
		if err == datas.ErrOptimisticLockFailed {
			cli.PrintErrf("routine %s failed to lock\n", name)
			continue
		}

		if err == nil {
			cli.PrintErrf("routine %s committed successfully\n", name)
		}

		return err
	}

	return fmt.Errorf("Couldn't commit")
}

func (t Test) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}
