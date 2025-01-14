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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
)

func NewGlobalStateStoreForDb(ctx context.Context, dbName string, db *doltdb.DoltDB) (GlobalStateImpl, error) {
	branches, err := db.GetBranches(ctx)
	if err != nil {
		return GlobalStateImpl{}, err
	}

	remotes, err := db.GetRemoteRefs(ctx)
	if err != nil {
		return GlobalStateImpl{}, err
	}

	eg2, egCtx := errgroup.WithContext(ctx)
	eg2.SetLimit(128)

	rootRefs := make([]ref.DoltRef, 0, len(branches)+len(remotes))
	rootRefs = append(rootRefs, branches...)
	rootRefs = append(rootRefs, remotes...)

	rootChan := make(chan doltdb.Rootish, len(rootRefs))

	wg := sync.WaitGroup{}

	eg, _ := errgroup.WithContext(egCtx)
	eg.Go(func() error {
		wg.Wait()
		close(rootChan)
		return nil
	})

	for _, b := range rootRefs {
		wg.Add(1)

		eg2.Go(func() error {
			defer wg.Done()

			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			switch b.GetType() {
			case ref.BranchRefType:
				wsRef, rerr := ref.WorkingSetRefForHead(b)
				if rerr != nil {
					return rerr
				}

				ws, rerr := db.ResolveWorkingSet(egCtx, wsRef)
				if rerr == doltdb.ErrWorkingSetNotFound {
					// use the branch head if there isn't a working set for it
					cm, rerr := db.ResolveCommitRef(egCtx, b)
					if rerr != nil {
						return rerr
					}
					rootChan <- cm
				} else if err != nil {
					return rerr
				} else {
					rootChan <- ws
				}
			case ref.RemoteRefType:
				cm, rerr := db.ResolveCommitRef(egCtx, b)
				if rerr != nil {
					return rerr
				}
				rootChan <- cm
			}
			return nil
		})
	}

	err = eg2.Wait()
	if err != nil {
		return GlobalStateImpl{}, err
	}

	err = eg.Wait()
	if err != nil {
		return GlobalStateImpl{}, err
	}

	var roots []doltdb.Rootish
	for root := range rootChan {
		roots = append(roots, root)
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
