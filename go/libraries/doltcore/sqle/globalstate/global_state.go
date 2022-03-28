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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

type StateProvider interface {
	GetGlobalState() GlobalState
}

func NewGlobalStateStore() GlobalState {
	return GlobalState{
		trackerMap: make(map[ref.WorkingSetRef]AutoIncrementTracker),
		mu:         &sync.Mutex{},
	}
}

type GlobalState struct {
	trackerMap map[ref.WorkingSetRef]AutoIncrementTracker
	mu         *sync.Mutex
}

func (g GlobalState) GetAutoIncrementTracker(ctx context.Context, ws *doltdb.WorkingSet) (AutoIncrementTracker, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	ait, ok := g.trackerMap[ws.Ref()]
	if ok {
		return ait, nil
	}

	var err error
	ait, err = NewAutoIncrementTracker(ctx, ws)
	if err != nil {
		return AutoIncrementTracker{}, err
	}
	g.trackerMap[ws.Ref()] = ait

	return ait, nil
}
