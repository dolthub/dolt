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

package globalstate

import (
	"context"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

type StateProvider interface {
	GetGlobalState() GlobalState
}

func NewGlobalStateStoreForDb(ctx context.Context, db *doltdb.DoltDB) (GlobalState, error) {
	branches, err := db.GetBranches(ctx)
	if err != nil {
		return GlobalState{}, err
	}

	var wses []*doltdb.WorkingSet
	for _, b := range branches {
		wsRef, err := ref.WorkingSetRefForHead(b)
		if err != nil {
			return GlobalState{}, err
		}

		ws, err := db.ResolveWorkingSet(ctx, wsRef)
		if err == doltdb.ErrWorkingSetNotFound {
			// skip, continue working on other branches
			continue
		} else if err != nil {
			return GlobalState{}, err
		}

		wses = append(wses, ws)
	}

	tracker, err := NewAutoIncrementTracker(ctx, wses...)
	if err != nil {
		return GlobalState{}, err
	}

	return GlobalState{
		aiTracker: tracker,
		mu:        &sync.Mutex{},
	}, nil
}

type GlobalState struct {
	aiTracker AutoIncrementTracker
	mu        *sync.Mutex
}

func (g GlobalState) GetAutoIncrementTracker(ctx *sql.Context) (AutoIncrementTracker, error) {
	return g.aiTracker, nil
}
