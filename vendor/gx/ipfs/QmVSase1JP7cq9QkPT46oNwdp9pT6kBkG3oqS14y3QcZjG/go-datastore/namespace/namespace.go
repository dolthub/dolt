package namespace

import (
	"fmt"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	ktds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/keytransform"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// PrefixTransform constructs a KeyTransform with a pair of functions that
// add or remove the given prefix key.
//
// Warning: will panic if prefix not found when it should be there. This is
// to avoid insidious data inconsistency errors.
func PrefixTransform(prefix ds.Key) ktds.KeyTransform {
	return &ktds.Pair{

		// Convert adds the prefix
		Convert: func(k ds.Key) ds.Key {
			return prefix.Child(k)
		},

		// Invert removes the prefix. panics if prefix not found.
		Invert: func(k ds.Key) ds.Key {
			if prefix.String() == "/" {
				return k
			}

			if !prefix.IsAncestorOf(k) {
				fmt.Errorf("Expected prefix (%s) in key (%s)", prefix, k)
				panic("expected prefix not found")
			}

			s := k.String()[len(prefix.String()):]
			return ds.RawKey(s)
		},
	}
}

// Wrap wraps a given datastore with a key-prefix.
func Wrap(child ds.Datastore, prefix ds.Key) *datastore {
	if child == nil {
		panic("child (ds.Datastore) is nil")
	}

	d := ktds.Wrap(child, PrefixTransform(prefix))
	return &datastore{Datastore: d, raw: child, prefix: prefix}
}

type datastore struct {
	prefix ds.Key
	raw    ds.Datastore
	ktds.Datastore
}

// Query implements Query, inverting keys on the way back out.
// This function assumes that child datastore.Query returns ordered results
func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	q.Prefix = d.prefix.Child(ds.NewKey(q.Prefix)).String()
	qr, err := d.raw.Query(q)
	if err != nil {
		return nil, err
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			for {
				r, ok := qr.NextSync()
				if !ok {
					return r, false
				}
				if r.Error != nil {
					return r, true
				}
				k := ds.RawKey(r.Entry.Key)
				if !d.prefix.IsAncestorOf(k) {
					return dsq.Result{}, false
				}

				r.Entry.Key = d.Datastore.InvertKey(k).String()
				return r, true
			}
		},
		Close: func() error {
			return qr.Close()
		},
	}), nil
}

func (d *datastore) Batch() (ds.Batch, error) {
	if bds, ok := d.Datastore.(ds.Batching); ok {
		return bds.Batch()
	}

	return nil, ds.ErrBatchUnsupported
}
