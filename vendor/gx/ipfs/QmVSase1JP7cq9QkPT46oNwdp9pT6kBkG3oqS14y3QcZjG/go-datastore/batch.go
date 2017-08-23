package datastore

// basicBatch implements the transaction interface for datastores who do
// not have any sort of underlying transactional support
type basicBatch struct {
	puts    map[Key]interface{}
	deletes map[Key]struct{}

	target Datastore
}

func NewBasicBatch(ds Datastore) Batch {
	return &basicBatch{
		puts:    make(map[Key]interface{}),
		deletes: make(map[Key]struct{}),
		target:  ds,
	}
}

func (bt *basicBatch) Put(key Key, val interface{}) error {
	bt.puts[key] = val
	return nil
}

func (bt *basicBatch) Delete(key Key) error {
	bt.deletes[key] = struct{}{}
	return nil
}

func (bt *basicBatch) Commit() error {
	for k, val := range bt.puts {
		if err := bt.target.Put(k, val); err != nil {
			return err
		}
	}

	for k, _ := range bt.deletes {
		if err := bt.target.Delete(k); err != nil {
			return err
		}
	}

	return nil
}
