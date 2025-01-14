// Copyright 2023 Dolthub, Inc.
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

package dsess

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
)

func NewGlobalStateStoreForDb(ctx context.Context, dbName string, db *doltdb.DoltDB) (GlobalStateImpl, error) {
	start := time.Now()
	branches, err := db.GetBranches(ctx)
	if err != nil {
		fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: get branches error: elapsed: %v\n", time.Since(start))
		return GlobalStateImpl{}, err
	}

	remotes, err := db.GetRemoteRefs(ctx)
	if err != nil {
		fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: get remote refs error: elapsed: %v\n", time.Since(start))
		return GlobalStateImpl{}, err
	}

	rootRefs := make([]ref.DoltRef, 0, len(branches)+len(remotes))
	rootRefs = append(rootRefs, branches...)
	rootRefs = append(rootRefs, remotes...)

	rootRefsChan := make(chan doltdb.Rootish, len(rootRefs))
	eg, egCtx := errgroup.WithContext(ctx)
	wg := sync.WaitGroup{}

	eg.Go(func() error {
		defer close(rootRefsChan)
		wg.Wait()
		return nil
	})

	for _, b := range rootRefs {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			switch b.GetType() {
			case ref.BranchRefType:
				wsRef, err := ref.WorkingSetRefForHead(b)
				if err != nil {
					fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: working set error: elapsed: %v\n", time.Since(start))
					return err
				}

				ws, err := db.ResolveWorkingSet(egCtx, wsRef)
				if err == doltdb.ErrWorkingSetNotFound {
					// use the branch head if there isn't a working set for it
					cm, err := db.ResolveCommitRef(egCtx, b)
					if err != nil {
						fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: resolve commit error: elapsed: %v\n", time.Since(start))
						return err
					}
					rootRefsChan <- cm
				} else if err != nil {
					fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: resolve working set error: elapsed: %v\n", time.Since(start))
					return err
				} else {
					rootRefsChan <- ws
				}
			case ref.RemoteRefType:
				cm, err := db.ResolveCommitRef(egCtx, b)
				if err != nil {
					fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: resolve commit remote ref error: elapsed: %v\n", time.Since(start))
					return err
				}
				rootRefsChan <- cm
			}
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return GlobalStateImpl{}, err
	}

	if len(rootRefsChan) != len(rootRefs) {
		fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: rootRefsChan does not equal rootRefs\n")
	}

	fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: success: elapsed: %v\n", time.Since(start))

	var roots []doltdb.Rootish
	for rootRef := range rootRefsChan {
		roots = append(roots, rootRef)
	}

	if len(roots) != len(rootRefs) {
		fmt.Fprintf(color.Output, "DUSTIN: NewGlobalStateStoreForDb: roots does not equal rootRefs\n")
	}

	tracker, err := NewAutoIncrementTracker(ctx, dbName, roots...)
	if err != nil {
		return GlobalStateImpl{}, err
	}

	return GlobalStateImpl{
		aiTracker: tracker,
		mu:        &sync.Mutex{},
	}, nil
}

type GlobalStateImpl struct {
	aiTracker globalstate.AutoIncrementTracker
	mu        *sync.Mutex
}

var _ globalstate.GlobalState = GlobalStateImpl{}

func (g GlobalStateImpl) AutoIncrementTracker(ctx *sql.Context) (globalstate.AutoIncrementTracker, error) {
	return g.aiTracker, nil
}
