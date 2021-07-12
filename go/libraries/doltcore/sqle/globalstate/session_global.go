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
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

type GlobalState interface {
	GetAutoIncrementTracker(wsref ref.WorkingSetRef) AutoIncrementTracker
}

func NewSessionGlobalInMemStore() GlobalState {
	return &globalStateImpl{
		trackerMap: make(map[ref.WorkingSetRef]AutoIncrementTracker),
	}
}

type globalStateImpl struct {
	trackerMap map[ref.WorkingSetRef]AutoIncrementTracker
	mu         sync.Mutex
}

var _ GlobalState = (*globalStateImpl)(nil)

func (g *globalStateImpl) GetAutoIncrementTracker(wsref ref.WorkingSetRef) AutoIncrementTracker {
	g.mu.Lock()
	defer g.mu.Unlock()

	_, ok := g.trackerMap[wsref]
	if !ok {
		g.trackerMap[wsref] = NewAutoIncrementTracker()
	}

	return g.trackerMap[wsref]
}
