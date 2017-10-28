// package wantlist implements an object for bitswap that contains the keys
// that a given peer wants.
package wantlist

import (
	"sort"
	"sync"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
)

type ThreadSafe struct {
	lk  sync.RWMutex
	set map[string]*Entry
}

// not threadsafe
type Wantlist struct {
	set map[string]*Entry
}

type Entry struct {
	Cid      *cid.Cid
	Priority int

	SesTrk map[uint64]struct{}
}

// NewRefEntry creates a new reference tracked wantlist entry
func NewRefEntry(c *cid.Cid, p int) *Entry {
	return &Entry{
		Cid:      c,
		Priority: p,
		SesTrk:   make(map[uint64]struct{}),
	}
}

type entrySlice []*Entry

func (es entrySlice) Len() int           { return len(es) }
func (es entrySlice) Swap(i, j int)      { es[i], es[j] = es[j], es[i] }
func (es entrySlice) Less(i, j int) bool { return es[i].Priority > es[j].Priority }

func NewThreadSafe() *ThreadSafe {
	return &ThreadSafe{
		set: make(map[string]*Entry),
	}
}

func New() *Wantlist {
	return &Wantlist{
		set: make(map[string]*Entry),
	}
}

// Add adds the given cid to the wantlist with the specified priority, governed
// by the session ID 'ses'.  if a cid is added under multiple session IDs, then
// it must be removed by each of those sessions before it is no longer 'in the
// wantlist'. Calls to Add are idempotent given the same arguments. Subsequent
// calls with different values for priority will not update the priority
// TODO: think through priority changes here
// Add returns true if the cid did not exist in the wantlist before this call
// (even if it was under a different session)
func (w *ThreadSafe) Add(c *cid.Cid, priority int, ses uint64) bool {
	w.lk.Lock()
	defer w.lk.Unlock()
	k := c.KeyString()
	if e, ok := w.set[k]; ok {
		e.SesTrk[ses] = struct{}{}
		return false
	}

	w.set[k] = &Entry{
		Cid:      c,
		Priority: priority,
		SesTrk:   map[uint64]struct{}{ses: struct{}{}},
	}

	return true
}

// AddEntry adds given Entry to the wantlist. For more information see Add method.
func (w *ThreadSafe) AddEntry(e *Entry, ses uint64) bool {
	w.lk.Lock()
	defer w.lk.Unlock()
	k := e.Cid.KeyString()
	if ex, ok := w.set[k]; ok {
		ex.SesTrk[ses] = struct{}{}
		return false
	}
	w.set[k] = e
	e.SesTrk[ses] = struct{}{}
	return true
}

// Remove removes the given cid from being tracked by the given session.
// 'true' is returned if this call to Remove removed the final session ID
// tracking the cid. (meaning true will be returned iff this call caused the
// value of 'Contains(c)' to change from true to false)
func (w *ThreadSafe) Remove(c *cid.Cid, ses uint64) bool {
	w.lk.Lock()
	defer w.lk.Unlock()
	k := c.KeyString()
	e, ok := w.set[k]
	if !ok {
		return false
	}

	delete(e.SesTrk, ses)
	if len(e.SesTrk) == 0 {
		delete(w.set, k)
		return true
	}
	return false
}

// Contains returns true if the given cid is in the wantlist tracked by one or
// more sessions
func (w *ThreadSafe) Contains(k *cid.Cid) (*Entry, bool) {
	w.lk.RLock()
	defer w.lk.RUnlock()
	e, ok := w.set[k.KeyString()]
	return e, ok
}

func (w *ThreadSafe) Entries() []*Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	return es
}

func (w *ThreadSafe) SortedEntries() []*Entry {
	w.lk.RLock()
	defer w.lk.RUnlock()
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	sort.Sort(es)
	return es
}

func (w *ThreadSafe) Len() int {
	w.lk.RLock()
	defer w.lk.RUnlock()
	return len(w.set)
}

func (w *Wantlist) Len() int {
	return len(w.set)
}

func (w *Wantlist) Add(c *cid.Cid, priority int) bool {
	k := c.KeyString()
	if _, ok := w.set[k]; ok {
		return false
	}

	w.set[k] = &Entry{
		Cid:      c,
		Priority: priority,
	}

	return true
}

func (w *Wantlist) AddEntry(e *Entry) bool {
	k := e.Cid.KeyString()
	if _, ok := w.set[k]; ok {
		return false
	}
	w.set[k] = e
	return true
}

func (w *Wantlist) Remove(c *cid.Cid) bool {
	k := c.KeyString()
	_, ok := w.set[k]
	if !ok {
		return false
	}

	delete(w.set, k)
	return true
}

func (w *Wantlist) Contains(k *cid.Cid) (*Entry, bool) {
	e, ok := w.set[k.KeyString()]
	return e, ok
}

func (w *Wantlist) Entries() []*Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	return es
}

func (w *Wantlist) SortedEntries() []*Entry {
	var es entrySlice
	for _, e := range w.set {
		es = append(es, e)
	}
	sort.Sort(es)
	return es
}
