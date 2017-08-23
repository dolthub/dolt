package timecache

import (
	"container/list"
	"time"
)

type TimeCache struct {
	Q *list.List
	M map[string]time.Time

	span time.Duration
}

func NewTimeCache(span time.Duration) *TimeCache {
	return &TimeCache{
		Q:    list.New(),
		M:    make(map[string]time.Time),
		span: span,
	}
}

func (tc *TimeCache) Add(s string) {
	_, ok := tc.M[s]
	if ok {
		panic("putting the same entry twice not supported")
	}

	tc.sweep()

	tc.M[s] = time.Now()
	tc.Q.PushFront(s)
}

func (tc *TimeCache) sweep() {
	for {
		back := tc.Q.Back()
		if back == nil {
			return
		}

		v := back.Value.(string)
		t, ok := tc.M[v]
		if !ok {
			panic("inconsistent cache state")
		}

		if time.Since(t) > tc.span {
			tc.Q.Remove(back)
			delete(tc.M, v)
		} else {
			return
		}
	}
}

func (tc *TimeCache) Has(s string) bool {
	_, ok := tc.M[s]
	return ok
}
