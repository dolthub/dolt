package autobatch

import (
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type Datastore struct {
	child ds.Batching

	// TODO: discuss making ds.Batch implement the full ds.Datastore interface
	buffer           map[ds.Key]interface{}
	maxBufferEntries int
}

func NewAutoBatching(d ds.Batching, size int) *Datastore {
	return &Datastore{
		child:            d,
		buffer:           make(map[ds.Key]interface{}),
		maxBufferEntries: size,
	}
}

func (d *Datastore) Delete(k ds.Key) error {
	delete(d.buffer, k)

	return d.child.Delete(k)
}

func (d *Datastore) Get(k ds.Key) (interface{}, error) {
	val, ok := d.buffer[k]
	if ok {
		return val, nil
	}

	return d.child.Get(k)
}

func (d *Datastore) Put(k ds.Key, val interface{}) error {
	d.buffer[k] = val
	if len(d.buffer) > d.maxBufferEntries {
		return d.Flush()
	}
	return nil
}

func (d *Datastore) Flush() error {
	b, err := d.child.Batch()
	if err != nil {
		return err
	}

	for k, v := range d.buffer {
		err := b.Put(k, v)
		if err != nil {
			return err
		}
	}
	// clear out buffer
	d.buffer = make(map[ds.Key]interface{})

	return b.Commit()
}

func (d *Datastore) Has(k ds.Key) (bool, error) {
	_, ok := d.buffer[k]
	if ok {
		return true, nil
	}

	return d.child.Has(k)
}

func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	err := d.Flush()
	if err != nil {
		return nil, err
	}

	return d.child.Query(q)
}

var _ ds.Datastore = (*Datastore)(nil)
