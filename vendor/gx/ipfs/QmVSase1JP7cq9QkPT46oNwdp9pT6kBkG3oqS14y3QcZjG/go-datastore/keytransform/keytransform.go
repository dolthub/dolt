package keytransform

import (
	"io"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type Pair struct {
	Convert KeyMapping
	Invert  KeyMapping
}

func (t *Pair) ConvertKey(k ds.Key) ds.Key {
	return t.Convert(k)
}

func (t *Pair) InvertKey(k ds.Key) ds.Key {
	return t.Invert(k)
}

// ktds keeps a KeyTransform function
type ktds struct {
	child ds.Datastore

	KeyTransform
}

// Children implements ds.Shim
func (d *ktds) Children() []ds.Datastore {
	return []ds.Datastore{d.child}
}

// Put stores the given value, transforming the key first.
func (d *ktds) Put(key ds.Key, value interface{}) (err error) {
	return d.child.Put(d.ConvertKey(key), value)
}

// Get returns the value for given key, transforming the key first.
func (d *ktds) Get(key ds.Key) (value interface{}, err error) {
	return d.child.Get(d.ConvertKey(key))
}

// Has returns whether the datastore has a value for a given key, transforming
// the key first.
func (d *ktds) Has(key ds.Key) (exists bool, err error) {
	return d.child.Has(d.ConvertKey(key))
}

// Delete removes the value for given key
func (d *ktds) Delete(key ds.Key) (err error) {
	return d.child.Delete(d.ConvertKey(key))
}

// Query implements Query, inverting keys on the way back out.
func (d *ktds) Query(q dsq.Query) (dsq.Results, error) {
	qr, err := d.child.Query(q)
	if err != nil {
		return nil, err
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			r, ok := qr.NextSync()
			if !ok {
				return r, false
			}
			if r.Error == nil {
				r.Entry.Key = d.InvertKey(ds.RawKey(r.Entry.Key)).String()
			}
			return r, true
		},
		Close: func() error {
			return qr.Close()
		},
	}), nil
}

func (d *ktds) Close() error {
	if c, ok := d.child.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (d *ktds) Batch() (ds.Batch, error) {
	bds, ok := d.child.(ds.Batching)
	if !ok {
		return nil, ds.ErrBatchUnsupported
	}

	childbatch, err := bds.Batch()
	if err != nil {
		return nil, err
	}
	return &transformBatch{
		dst: childbatch,
		f:   d.ConvertKey,
	}, nil
}

type transformBatch struct {
	dst ds.Batch

	f KeyMapping
}

func (t *transformBatch) Put(key ds.Key, val interface{}) error {
	return t.dst.Put(t.f(key), val)
}

func (t *transformBatch) Delete(key ds.Key) error {
	return t.dst.Delete(t.f(key))
}

func (t *transformBatch) Commit() error {
	return t.dst.Commit()
}
