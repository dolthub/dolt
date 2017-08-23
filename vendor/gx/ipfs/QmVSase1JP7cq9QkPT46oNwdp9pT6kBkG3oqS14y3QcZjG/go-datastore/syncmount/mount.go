// Package mount provides a Datastore that has other Datastores
// mounted at various key prefixes and is threadsafe
package syncmount

import (
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

var (
	ErrNoMount = errors.New("no datastore mounted for this key")
)

type Mount struct {
	Prefix    ds.Key
	Datastore ds.Datastore
}

func New(mounts []Mount) *Datastore {
	// make a copy so we're sure it doesn't mutate
	m := make([]Mount, len(mounts))
	for i, v := range mounts {
		m[i] = v
	}
	sort.Slice(m, func(i, j int) bool { return m[i].Prefix.String() > m[j].Prefix.String() })
	return &Datastore{mounts: m}
}

type Datastore struct {
	mounts []Mount
	lk     sync.Mutex
}

var _ ds.Datastore = (*Datastore)(nil)

func (d *Datastore) lookup(key ds.Key) (ds.Datastore, ds.Key, ds.Key) {
	d.lk.Lock()
	defer d.lk.Unlock()
	for _, m := range d.mounts {
		if m.Prefix.Equal(key) || m.Prefix.IsAncestorOf(key) {
			s := strings.TrimPrefix(key.String(), m.Prefix.String())
			k := ds.NewKey(s)
			return m.Datastore, m.Prefix, k
		}
	}
	return nil, ds.NewKey("/"), key
}

// lookupAll returns all mounts that might contain keys that are descendant of <key>
//
// Matching: /ao/e
//
// /          B /ao/e
// /a/        not matching
// /ao/       B /e
// /ao/e/     A /
// /ao/e/uh/  A /
// /aoe/      not matching
func (d *Datastore) lookupAll(key ds.Key) (dst []ds.Datastore, mountpoint, rest []ds.Key) {
	d.lk.Lock()
	defer d.lk.Unlock()

	for _, m := range d.mounts {
		p := m.Prefix.String()
		if len(p) > 1 {
			p = p + "/"
		}

		if strings.HasPrefix(p, key.String()) {
			dst = append(dst, m.Datastore)
			mountpoint = append(mountpoint, m.Prefix)
			rest = append(rest, ds.NewKey("/"))
		} else if strings.HasPrefix(key.String(), p) {
			r := strings.TrimPrefix(key.String(), m.Prefix.String())

			dst = append(dst, m.Datastore)
			mountpoint = append(mountpoint, m.Prefix)
			rest = append(rest, ds.NewKey(r))
		}
	}
	return dst, mountpoint, rest
}

func (d *Datastore) Put(key ds.Key, value interface{}) error {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return ErrNoMount
	}
	return cds.Put(k, value)
}

func (d *Datastore) Get(key ds.Key) (value interface{}, err error) {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return nil, ds.ErrNotFound
	}
	return cds.Get(k)
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return false, nil
	}
	return cds.Has(k)
}

func (d *Datastore) Delete(key ds.Key) error {
	cds, _, k := d.lookup(key)
	if cds == nil {
		return ds.ErrNotFound
	}
	return cds.Delete(k)
}

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	if len(q.Filters) > 0 ||
		len(q.Orders) > 0 ||
		q.Limit > 0 ||
		q.Offset > 0 {
		// TODO this is still overly simplistic, but the only callers are
		// `ipfs refs local` and ipfs-ds-convert.
		return nil, errors.New("mount only supports listing all prefixed keys in random order")
	}
	prefix := ds.NewKey(q.Prefix)
	dses, mounts, rests := d.lookupAll(prefix)

	// current itorator state
	var res query.Results
	var mount ds.Key
	i := 0

	return query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			var r query.Result
			var more bool

			for try := true; try; try = len(dses) > i {
				if res == nil {
					if len(dses) <= i {
						//This should not happen normally
						return query.Result{}, false
					}

					dst := dses[i]
					mount = mounts[i]
					rest := rests[i]

					q2 := q
					q2.Prefix = rest.String()
					r, err := dst.Query(q2)
					if err != nil {
						return query.Result{Error: err}, false
					}
					res = r
				}

				r, more = res.NextSync()
				if !more {
					err := res.Close()
					if err != nil {
						return query.Result{Error: err}, false
					}
					res = nil

					i++
					more = len(dses) > i
				} else {
					break
				}
			}

			r.Key = mount.Child(ds.RawKey(r.Key)).String()
			return r, more
		},
		Close: func() error {
			if len(mounts) > i && res != nil {
				return res.Close()
			}
			return nil
		},
	}), nil
}

func (d *Datastore) IsThreadSafe() {}

func (d *Datastore) Close() error {
	for _, d := range d.mounts {
		if c, ok := d.Datastore.(io.Closer); ok {
			err := c.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type mountBatch struct {
	mounts map[string]ds.Batch
	lk     sync.Mutex

	d *Datastore
}

func (d *Datastore) Batch() (ds.Batch, error) {
	return &mountBatch{
		mounts: make(map[string]ds.Batch),
		d:      d,
	}, nil
}

func (mt *mountBatch) lookupBatch(key ds.Key) (ds.Batch, ds.Key, error) {
	mt.lk.Lock()
	defer mt.lk.Unlock()

	child, loc, rest := mt.d.lookup(key)
	t, ok := mt.mounts[loc.String()]
	if !ok {
		bds, ok := child.(ds.Batching)
		if !ok {
			return nil, ds.NewKey(""), ds.ErrBatchUnsupported
		}
		var err error
		t, err = bds.Batch()
		if err != nil {
			return nil, ds.NewKey(""), err
		}
		mt.mounts[loc.String()] = t
	}
	return t, rest, nil
}

func (mt *mountBatch) Put(key ds.Key, val interface{}) error {
	t, rest, err := mt.lookupBatch(key)
	if err != nil {
		return err
	}

	return t.Put(rest, val)
}

func (mt *mountBatch) Delete(key ds.Key) error {
	t, rest, err := mt.lookupBatch(key)
	if err != nil {
		return err
	}

	return t.Delete(rest)
}

func (mt *mountBatch) Commit() error {
	for _, t := range mt.mounts {
		err := t.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}
