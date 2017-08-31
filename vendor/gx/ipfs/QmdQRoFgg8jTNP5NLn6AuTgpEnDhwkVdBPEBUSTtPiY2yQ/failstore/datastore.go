package failstore

import (
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type Failstore struct {
	child   ds.Datastore
	errfunc func(string) error
}

func NewFailstore(c ds.Datastore, efunc func(string) error) *Failstore {
	return &Failstore{
		child:   c,
		errfunc: efunc,
	}
}

func (d *Failstore) Put(k ds.Key, val interface{}) error {
	err := d.errfunc("put")
	if err != nil {
		return err
	}

	return d.child.Put(k, val)
}

func (d *Failstore) Get(k ds.Key) (interface{}, error) {
	err := d.errfunc("get")
	if err != nil {
		return nil, err
	}

	return d.child.Get(k)
}

func (d *Failstore) Has(k ds.Key) (bool, error) {
	err := d.errfunc("has")
	if err != nil {
		return false, err
	}

	return d.child.Has(k)
}

func (d *Failstore) Delete(k ds.Key) error {
	err := d.errfunc("delete")
	if err != nil {
		return err
	}

	return d.child.Delete(k)
}

func (d *Failstore) Query(q dsq.Query) (dsq.Results, error) {
	err := d.errfunc("query")
	if err != nil {
		return nil, err
	}

	return d.child.Query(q)
}

type FailBatch struct {
	cb     ds.Batch
	dstore *Failstore
}

func (d *Failstore) Batch() (ds.Batch, error) {
	if err := d.errfunc("batch"); err != nil {
		return nil, err
	}

	b, err := d.child.(ds.Batching).Batch()
	if err != nil {
		return nil, err
	}

	return &FailBatch{
		cb:     b,
		dstore: d,
	}, nil
}

func (b *FailBatch) Put(k ds.Key, val interface{}) error {
	if err := b.dstore.errfunc("batch-put"); err != nil {
		return err
	}

	return b.cb.Put(k, val)
}

func (b *FailBatch) Delete(k ds.Key) error {
	if err := b.dstore.errfunc("batch-delete"); err != nil {
		return err
	}

	return b.cb.Delete(k)
}

func (b *FailBatch) Commit() error {
	if err := b.dstore.errfunc("batch-commit"); err != nil {
		return err
	}

	return b.cb.Commit()
}
