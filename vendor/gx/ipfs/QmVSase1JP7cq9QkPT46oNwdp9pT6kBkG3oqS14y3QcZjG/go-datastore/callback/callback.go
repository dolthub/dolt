package callback

import (
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type Datastore struct {
	D ds.Datastore
	F func()
}

func Wrap(ds ds.Datastore, f func()) *Datastore {
	return &Datastore{ds, f}
}

func (c *Datastore) SetFunc(f func()) { c.F = f }

func (c *Datastore) Put(key ds.Key, value interface{}) (err error) {
	c.F()
	return c.D.Put(key, value)
}

func (c *Datastore) Get(key ds.Key) (value interface{}, err error) {
	c.F()
	return c.D.Get(key)
}

func (c *Datastore) Has(key ds.Key) (exists bool, err error) {
	c.F()
	return c.D.Has(key)
}

func (c *Datastore) Delete(key ds.Key) (err error) {
	c.F()
	return c.D.Delete(key)
}

func (c *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	c.F()
	return c.D.Query(q)
}
