package coalesce

import (
	"io"
	"sync"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// parent keys
var (
	putKey    = "put"
	getKey    = "get"
	hasKey    = "has"
	deleteKey = "delete"
)

type keySync struct {
	op    string
	k     ds.Key
	value interface{}
}

type valSync struct {
	val  interface{}
	err  error
	done chan struct{}
}

// Datastore uses golang-lru for internal storage.
type datastore struct {
	child ds.Datastore

	reqmu sync.Mutex
	req   map[keySync]*valSync
}

// Wrap wraps a given datastore with a coalescing datastore.
// All simultaenous requests which have the same keys will
// yield the exact same result. Note that this shares
// memory. It is not possible to copy a generic interface{}
func Wrap(d ds.Datastore) ds.Datastore {
	return &datastore{child: d, req: make(map[keySync]*valSync)}
}

// sync synchronizes requests for a given key.
func (d *datastore) sync(k keySync) (vs *valSync, found bool) {
	d.reqmu.Lock()
	vs, found = d.req[k]
	if !found {
		vs = &valSync{done: make(chan struct{})}
		d.req[k] = vs
	}
	d.reqmu.Unlock()

	// if we did find one, wait till it's done.
	if found {
		<-vs.done
	}
	return vs, found
}

// sync synchronizes requests for a given key.
func (d *datastore) syncDone(k keySync) {

	d.reqmu.Lock()
	vs, found := d.req[k]
	if !found {
		panic("attempt to syncDone non-existent request")
	}
	delete(d.req, k)
	d.reqmu.Unlock()

	// release all the waiters.
	close(vs.done)
}

// Put stores the object `value` named by `key`.
func (d *datastore) Put(key ds.Key, value interface{}) (err error) {
	ks := keySync{putKey, key, value}
	vs, found := d.sync(ks)
	if !found {
		vs.err = d.child.Put(key, value)
		d.syncDone(ks)
	}
	return err
}

// Get retrieves the object `value` named by `key`.
func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	ks := keySync{getKey, key, nil}
	vs, found := d.sync(ks)
	if !found {
		vs.val, vs.err = d.child.Get(key)
		d.syncDone(ks)
	}
	return vs.val, vs.err
}

// Has returns whether the `key` is mapped to a `value`.
func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	ks := keySync{hasKey, key, nil}
	vs, found := d.sync(ks)
	if !found {
		vs.val, vs.err = d.child.Has(key)
		d.syncDone(ks)
	}
	return vs.val.(bool), vs.err
}

// Delete removes the value for given `key`.
func (d *datastore) Delete(key ds.Key) (err error) {
	ks := keySync{deleteKey, key, nil}
	vs, found := d.sync(ks)
	if !found {
		vs.err = d.child.Delete(key)
		d.syncDone(ks)
	}
	return vs.err
}

// Query returns a list of keys in the datastore
func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	// query not coalesced yet.
	return d.child.Query(q)
}

func (d *datastore) Close() error {
	d.reqmu.Lock()
	defer d.reqmu.Unlock()

	for _, s := range d.req {
		<-s.done
	}
	if c, ok := d.child.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
