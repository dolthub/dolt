package timecache

import (
	"io"
	"sync"
	"time"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// op keys
var (
	putKey    = "put"
	getKey    = "get"
	hasKey    = "has"
	deleteKey = "delete"
)

type datastore struct {
	cache ds.Datastore
	ttl   time.Duration

	ttlmu sync.Mutex
	ttls  map[ds.Key]time.Time
}

func WithTTL(ttl time.Duration) *datastore {
	return WithCache(ds.NewMapDatastore(), ttl)
}

// WithCache wraps a given datastore as a timecache.
// Get + Has requests are considered expired after a TTL.
func WithCache(d ds.Datastore, ttl time.Duration) *datastore {
	return &datastore{cache: d, ttl: ttl, ttls: make(map[ds.Key]time.Time)}
}

func (d *datastore) gc() {
	var now = time.Now()
	var del []ds.Key

	// remove all expired ttls.
	d.ttlmu.Lock()
	for k, ttl := range d.ttls {
		if now.After(ttl) {
			delete(d.ttls, k)
			del = append(del, k)
		}
	}
	d.ttlmu.Unlock()

	for _, k := range del {
		d.cache.Delete(k)
	}
}

func (d *datastore) ttlPut(key ds.Key) {
	d.ttlmu.Lock()
	d.ttls[key] = time.Now().Add(d.ttl)
	d.ttlmu.Unlock()
}

func (d *datastore) ttlDelete(key ds.Key) {
	d.ttlmu.Lock()
	delete(d.ttls, key)
	d.ttlmu.Unlock()
}

// Put stores the object `value` named by `key`.
func (d *datastore) Put(key ds.Key, value interface{}) (err error) {
	err = d.cache.Put(key, value)
	d.ttlPut(key)
	return err
}

// Get retrieves the object `value` named by `key`.
func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	d.gc()
	return d.cache.Get(key)
}

// Has returns whether the `key` is mapped to a `value`.
func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	d.gc()
	return d.cache.Has(key)
}

// Delete removes the value for given `key`.
func (d *datastore) Delete(key ds.Key) (err error) {
	d.ttlDelete(key)
	return d.cache.Delete(key)
}

// Query returns a list of keys in the datastore
func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	return d.cache.Query(q)
}

func (d *datastore) Close() error {
	if c, ok := d.cache.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
