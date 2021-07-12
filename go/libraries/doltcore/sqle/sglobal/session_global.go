package sglobal

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
