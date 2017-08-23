package retry

import (
	"fmt"
	"time"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

type Datastore struct {
	TempErrFunc func(error) bool
	Retries     int
	Delay       time.Duration

	ds.Batching
}

var errFmtString = "ran out of retries trying to get past temporary error: %s"

func (d *Datastore) runOp(op func() error) error {
	err := op()
	if err == nil || !d.TempErrFunc(err) {
		return err
	}

	for i := 0; i < d.Retries; i++ {
		time.Sleep(time.Duration(i+1) * d.Delay)

		err = op()
		if err == nil || !d.TempErrFunc(err) {
			return err
		}
	}

	return fmt.Errorf(errFmtString, err)
}

func (d *Datastore) Get(k ds.Key) (interface{}, error) {
	var val interface{}
	err := d.runOp(func() error {
		var err error
		val, err = d.Batching.Get(k)
		return err
	})

	return val, err
}

func (d *Datastore) Put(k ds.Key, val interface{}) error {
	return d.runOp(func() error {
		return d.Batching.Put(k, val)
	})
}

func (d *Datastore) Has(k ds.Key) (bool, error) {
	var has bool
	err := d.runOp(func() error {
		var err error
		has, err = d.Batching.Has(k)
		return err
	})
	return has, err
}
