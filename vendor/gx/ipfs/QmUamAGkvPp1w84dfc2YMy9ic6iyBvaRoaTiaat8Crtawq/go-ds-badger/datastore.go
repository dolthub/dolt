package badger

import (
	badger "gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger"

	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

type datastore struct {
	DB *badger.KV
}

// Options are the badger datastore options, reexported here for convenience.
type Options badger.Options

var DefaultOptions Options = Options(badger.DefaultOptions)

// NewDatastore creates a new badger datastore.
//
// DO NOT set the Dir and/or ValuePath fields of opt, they will be set for you.
func NewDatastore(path string, options *Options) (*datastore, error) {
	// Copy the options because we modify them.
	var opt badger.Options
	if options == nil {
		opt = badger.DefaultOptions
	} else {
		opt = badger.Options(*options)
	}

	opt.Dir = path
	opt.ValueDir = path

	kv, err := badger.NewKV(&opt)
	if err != nil {
		return nil, err
	}

	return &datastore{
		DB: kv,
	}, nil
}

func (d *datastore) Put(key ds.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	return d.DB.Set(key.Bytes(), val, 0)
}

func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	var item badger.KVItem
	err = d.DB.Get(key.Bytes(), &item)
	if err != nil {
		return nil, err
	}
	var bytes []byte

	err = item.Value(func(b []byte) error {
		if b != nil {
			bytes = make([]byte, len(b))
			copy(bytes, b)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		has, err := d.Has(key)
		if err != nil {
			return nil, err
		}
		if has {
			return []byte{}, nil
		}

		return nil, ds.ErrNotFound
	}
	return bytes, nil
}

func (d *datastore) Has(key ds.Key) (bool, error) {
	return d.DB.Exists(key.Bytes())

}

func (d *datastore) Delete(key ds.Key) error {
	return d.DB.Delete(key.Bytes())
}

func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	return d.QueryNew(q)
}

func (d *datastore) QueryNew(q dsq.Query) (dsq.Results, error) {
	prefix := []byte(q.Prefix)
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = !q.KeysOnly
	it := d.DB.NewIterator(opt)
	it.Seek([]byte(q.Prefix))
	if q.Offset > 0 {
		for j := 0; j < q.Offset; j++ {
			it.Next()
		}
	}

	qrb := dsq.NewResultBuilder(q)

	qrb.Process.Go(func(worker goprocess.Process) {
		defer it.Close()

		for sent := 0; it.ValidForPrefix(prefix); sent++ {
			if qrb.Query.Limit > 0 && sent >= qrb.Query.Limit {
				break
			}

			item := it.Item()

			k := string(item.Key())
			e := dsq.Entry{Key: k}

			if !q.KeysOnly {
				item.Value(func(b []byte) error {
					if b != nil {
						bytes := make([]byte, len(b))
						copy(bytes, b)
						e.Value = bytes
					}
					return nil
				})
			}

			select {
			case qrb.Output <- dsq.Result{Entry: e}:
			case <-worker.Closing(): // client told us to close early
				return
			}
			it.Next()
		}

		return
	})

	go qrb.Process.CloseAfterChildren()

	// Now, apply remaining things (filters, order)
	qr := qrb.Results()
	for _, f := range q.Filters {
		qr = dsq.NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = dsq.NaiveOrder(qr, o)
	}

	return qr, nil
}

func (d *datastore) Close() error {
	return d.DB.Close()
}

func (d *datastore) IsThreadSafe() {}

type badgerBatch struct {
	entries []*badger.Entry
	db      *badger.KV
}

func (d *datastore) Batch() (ds.Batch, error) {
	return &badgerBatch{
		db: d.DB,
	}, nil
}

func (b *badgerBatch) Put(key ds.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	b.entries = badger.EntriesSet(b.entries, key.Bytes(), val)
	return nil
}

func (b *badgerBatch) Commit() error {
	return b.db.BatchSet(b.entries)
}

func (b *badgerBatch) Delete(key ds.Key) error {
	b.entries = badger.EntriesDelete(b.entries, key.Bytes())
	return nil
}
