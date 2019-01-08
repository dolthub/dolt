package repo

import (
	"sync"
)

// OnlyOne tracks open Repos by arbitrary key and returns the already
// open one.
type OnlyOne struct {
	mu     sync.Mutex
	active map[interface{}]*ref
}

// Open a Repo identified by key. If Repo is not already open, the
// open function is called, and the result is remember for further
// use.
//
// Key must be comparable, or Open will panic. Make sure to pick keys
// that are unique across different concrete Repo implementations,
// e.g. by creating a local type:
//
//     type repoKey string
//     r, err := o.Open(repoKey(path), open)
//
// Call Repo.Close when done.
func (o *OnlyOne) Open(key interface{}, open func() (Repo, error)) (Repo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.active == nil {
		o.active = make(map[interface{}]*ref)
	}

	item, found := o.active[key]
	if !found {
		repo, err := open()
		if err != nil {
			return nil, err
		}
		item = &ref{
			parent: o,
			key:    key,
			Repo:   repo,
		}
		o.active[key] = item
	}
	item.refs++
	return item, nil
}

type ref struct {
	parent *OnlyOne
	key    interface{}
	refs   uint32
	Repo
}

var _ Repo = (*ref)(nil)

func (r *ref) Close() error {
	r.parent.mu.Lock()
	defer r.parent.mu.Unlock()

	r.refs--
	if r.refs > 0 {
		// others are holding it open
		return nil
	}

	// last one
	delete(r.parent.active, r.key)
	return r.Repo.Close()
}
