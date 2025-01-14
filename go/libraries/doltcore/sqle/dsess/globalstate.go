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

	rootRefs := make([]ref.DoltRef, 0, len(branches)+len(remotes))
	rootRefs = append(rootRefs, branches...)
	rootRefs = append(rootRefs, remotes...)

	rootRefsChan := make(chan doltdb.Rootish, len(rootRefs))
	eg, egCtx := errgroup.WithContext(ctx)
	wg := sync.WaitGroup{}

	//eg.Go(func() error {
	//	defer close(rootRefsChan)
	//	wg.Wait()
	//	return nil
	//})

	for _, b := range rootRefs {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			switch b.GetType() {
			case ref.BranchRefType:
				wsRef, err := ref.WorkingSetRefForHead(b)
				if err != nil {
					return err
				}

				ws, err := db.ResolveWorkingSet(egCtx, wsRef)
				if err == doltdb.ErrWorkingSetNotFound {
					// use the branch head if there isn't a working set for it
					cm, err := db.ResolveCommitRef(egCtx, b)
					if err != nil {
						return err
					}
					rootRefsChan <- cm
				} else if err != nil {
					return err
				} else {
					rootRefsChan <- ws
				}
			case ref.RemoteRefType:
				cm, err := db.ResolveCommitRef(egCtx, b)
				if err != nil {
					return err
				}
				rootRefsChan <- cm
			}
			return nil
		})
	}

	wg.Wait()
	close(rootRefsChan)
	err = eg.Wait()
	if err != nil {
		return GlobalStateImpl{}, err
	}

	var roots []doltdb.Rootish
	for rootRef := range rootRefsChan {
		roots = append(roots, rootRef)
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
