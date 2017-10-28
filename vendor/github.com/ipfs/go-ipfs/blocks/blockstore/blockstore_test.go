package blockstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	dshelp "github.com/ipfs/go-ipfs/thirdparty/ds-help"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	ds_sync "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
)

func TestGetWhenKeyNotPresent(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(c)

	if bl != nil {
		t.Error("nil block expected")
	}
	if err == nil {
		t.Error("error expected, got nil")
	}
}

func TestGetWhenKeyIsNil(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	_, err := bs.Get(nil)
	if err != ErrNotFound {
		t.Fail()
	}
}

func TestPutThenGetBlock(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	block := blocks.NewBlock([]byte("some data"))

	err := bs.Put(block)
	if err != nil {
		t.Fatal(err)
	}

	blockFromBlockstore, err := bs.Get(block.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(block.RawData(), blockFromBlockstore.RawData()) {
		t.Fail()
	}
}

func TestHashOnRead(t *testing.T) {
	orginalDebug := u.Debug
	defer (func() {
		u.Debug = orginalDebug
	})()
	u.Debug = false

	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	bl := blocks.NewBlock([]byte("some data"))
	blBad, err := blocks.NewBlockWithCid([]byte("some other data"), bl.Cid())
	if err != nil {
		t.Fatal("debug is off, still got an error")
	}
	bl2 := blocks.NewBlock([]byte("some other data"))
	bs.Put(blBad)
	bs.Put(bl2)
	bs.HashOnRead(true)

	if _, err := bs.Get(bl.Cid()); err != ErrHashMismatch {
		t.Fatalf("expected '%v' got '%v'\n", ErrHashMismatch, err)
	}

	if b, err := bs.Get(bl2.Cid()); err != nil || b.String() != bl2.String() {
		t.Fatal("got wrong blocks")
	}
}

func newBlockStoreWithKeys(t *testing.T, d ds.Datastore, N int) (Blockstore, []*cid.Cid) {
	if d == nil {
		d = ds.NewMapDatastore()
	}
	bs := NewBlockstore(ds_sync.MutexWrap(d))

	keys := make([]*cid.Cid, N)
	for i := 0; i < N; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(block)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = block.Cid()
	}
	return bs, keys
}

func collect(ch <-chan *cid.Cid) []*cid.Cid {
	var keys []*cid.Cid
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

func TestAllKeysSimple(t *testing.T) {
	bs, keys := newBlockStoreWithKeys(t, nil, 100)

	ctx := context.Background()
	ch, err := bs.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys2 := collect(ch)

	// for _, k2 := range keys2 {
	// 	t.Log("found ", k2.B58String())
	// }

	expectMatches(t, keys, keys2)
}

func TestAllKeysRespectsContext(t *testing.T) {
	N := 100

	d := &queryTestDS{ds: ds.NewMapDatastore()}
	bs, _ := newBlockStoreWithKeys(t, d, N)

	started := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	errors := make(chan error, 100)

	getKeys := func(ctx context.Context) {
		started <- struct{}{}
		ch, err := bs.AllKeysChan(ctx) // once without cancelling
		if err != nil {
			errors <- err
		}
		_ = collect(ch)
		done <- struct{}{}
		errors <- nil // a nil one to signal break
	}

	var results dsq.Results
	var resultsmu = make(chan struct{})
	resultChan := make(chan dsq.Result)
	d.SetFunc(func(q dsq.Query) (dsq.Results, error) {
		results = dsq.ResultsWithChan(q, resultChan)
		resultsmu <- struct{}{}
		return results, nil
	})

	go getKeys(context.Background())

	// make sure it's waiting.
	<-started
	<-resultsmu
	select {
	case <-done:
		t.Fatal("sync is wrong")
	case <-results.Process().Closing():
		t.Fatal("should not be closing")
	case <-results.Process().Closed():
		t.Fatal("should not be closed")
	default:
	}

	e := dsq.Entry{Key: BlockPrefix.ChildString("foo").String()}
	resultChan <- dsq.Result{Entry: e} // let it go.
	close(resultChan)
	<-done                       // should be done now.
	<-results.Process().Closed() // should be closed now

	// print any errors
	for err := range errors {
		if err == nil {
			break
		}
		t.Error(err)
	}

}

func TestErrValueTypeMismatch(t *testing.T) {
	block := blocks.NewBlock([]byte("some data"))

	datastore := ds.NewMapDatastore()
	k := BlockPrefix.Child(dshelp.CidToDsKey(block.Cid()))
	datastore.Put(k, "data that isn't a block!")

	blockstore := NewBlockstore(ds_sync.MutexWrap(datastore))

	_, err := blockstore.Get(block.Cid())
	if err != ErrValueTypeMismatch {
		t.Fatal(err)
	}
}

func expectMatches(t *testing.T, expect, actual []*cid.Cid) {

	if len(expect) != len(actual) {
		t.Errorf("expect and actual differ: %d != %d", len(expect), len(actual))
	}
	for _, ek := range expect {
		found := false
		for _, ak := range actual {
			if ek.Equals(ak) {
				found = true
			}
		}
		if !found {
			t.Error("expected key not found: ", ek)
		}
	}
}

type queryTestDS struct {
	cb func(q dsq.Query) (dsq.Results, error)
	ds ds.Datastore
}

func (c *queryTestDS) SetFunc(f func(dsq.Query) (dsq.Results, error)) { c.cb = f }

func (c *queryTestDS) Put(key ds.Key, value interface{}) (err error) {
	return c.ds.Put(key, value)
}

func (c *queryTestDS) Get(key ds.Key) (value interface{}, err error) {
	return c.ds.Get(key)
}

func (c *queryTestDS) Has(key ds.Key) (exists bool, err error) {
	return c.ds.Has(key)
}

func (c *queryTestDS) Delete(key ds.Key) (err error) {
	return c.ds.Delete(key)
}

func (c *queryTestDS) Query(q dsq.Query) (dsq.Results, error) {
	if c.cb != nil {
		return c.cb(q)
	}
	return c.ds.Query(q)
}

func (c *queryTestDS) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(c), nil
}
