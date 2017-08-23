package sync

import (
	"fmt"
	"io"
	"os"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type datastore struct {
	child ds.Datastore
}

// Wrap shims a datastore such than _any_ operation failing triggers a panic
// This is useful for debugging invariants.
func Wrap(d ds.Datastore) ds.Shim {
	return &datastore{child: d}
}

func (d *datastore) Children() []ds.Datastore {
	return []ds.Datastore{d.child}
}

func (d *datastore) Put(key ds.Key, value interface{}) error {
	err := d.child.Put(key, value)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: Put failed")
	}
	return nil
}

func (d *datastore) Get(key ds.Key) (interface{}, error) {
	val, err := d.child.Get(key)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: Get failed")
	}
	return val, nil
}

func (d *datastore) Has(key ds.Key) (bool, error) {
	e, err := d.child.Has(key)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: Has failed")
	}
	return e, nil
}

func (d *datastore) Delete(key ds.Key) error {
	err := d.child.Delete(key)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: Delete failed")
	}
	return nil
}

func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	r, err := d.child.Query(q)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: Query failed")
	}
	return r, nil
}

func (d *datastore) Close() error {
	if c, ok := d.child.(io.Closer); ok {
		err := c.Close()
		if err != nil {
			fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
			panic("panic datastore: Close failed")
		}
	}
	return nil
}

func (d *datastore) Batch() (ds.Batch, error) {
	b, err := d.child.(ds.Batching).Batch()
	if err != nil {
		return nil, err
	}

	return &panicBatch{b}, nil
}

type panicBatch struct {
	t ds.Batch
}

func (p *panicBatch) Put(key ds.Key, val interface{}) error {
	err := p.t.Put(key, val)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: transaction put failed")
	}
	return nil
}

func (p *panicBatch) Delete(key ds.Key) error {
	err := p.t.Delete(key)
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: transaction delete failed")
	}
	return nil
}

func (p *panicBatch) Commit() error {
	err := p.t.Commit()
	if err != nil {
		fmt.Fprintf(os.Stdout, "panic datastore: %s", err)
		panic("panic datastore: transaction commit failed")
	}
	return nil
}
