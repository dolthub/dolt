package benchmark

import (
	"sync"
	"time"
)

type latencyMapEntry struct {
	count int
	dur   time.Duration
}

type LatencyMap struct {
	sync.Mutex
	stats map[string]*latencyMapEntry
}

func NewLatencyMap() *LatencyMap {
	m := &LatencyMap{}
	m.stats = make(map[string]*latencyMapEntry)
	return m
}

func (m *LatencyMap) Get(name string) (count int, dt time.Duration) {
	m.Mutex.Lock()
	l := m.stats[name]
	m.Mutex.Unlock()
	return l.count, l.dur
}

func (m *LatencyMap) Add(name string, dt time.Duration) {
	m.Mutex.Lock()
	e := m.stats[name]
	if e == nil {
		e = new(latencyMapEntry)
		m.stats[name] = e
	}
	e.count++
	e.dur += dt
	m.Mutex.Unlock()
}

func (m *LatencyMap) Counts() map[string]int {
	r := make(map[string]int)
	m.Mutex.Lock()
	for k, v := range m.stats {
		r[k] = v.count
	}
	m.Mutex.Unlock()

	return r
}
