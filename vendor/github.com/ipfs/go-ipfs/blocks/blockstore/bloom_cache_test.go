package blockstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	context "context"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	syncds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
)

func testBloomCached(ctx context.Context, bs Blockstore) (*bloomcache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts := DefaultCacheOpts()
	opts.HasARCCacheSize = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*bloomcache), nil
	}
	return nil, err
}

func TestPutManyAddsToBloom(t *testing.T) {
	bs := NewBlockstore(syncds.MutexWrap(ds.NewMapDatastore()))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-cachedbs.rebuildChan:
	case <-ctx.Done():
		t.Fatalf("Timeout wating for rebuild: %d", cachedbs.bloom.ElementsAdded())
	}

	block1 := blocks.NewBlock([]byte("foo"))
	block2 := blocks.NewBlock([]byte("bar"))

	cachedbs.PutMany([]blocks.Block{block1})
	has, err := cachedbs.Has(block1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("added block is reported missing")
	}

	has, err = cachedbs.Has(block2.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("not added block is reported to be in blockstore")
	}
}

func TestReturnsErrorWhenSizeNegative(t *testing.T) {
	bs := NewBlockstore(syncds.MutexWrap(ds.NewMapDatastore()))
	_, err := bloomCached(context.Background(), bs, -1, 1)
	if err == nil {
		t.Fail()
	}
}
func TestHasIsBloomCached(t *testing.T) {
	cd := &callbackDatastore{f: func() {}, ds: ds.NewMapDatastore()}
	bs := NewBlockstore(syncds.MutexWrap(cd))

	for i := 0; i < 1000; i++ {
		bs.Put(blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i))))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-cachedbs.rebuildChan:
	case <-ctx.Done():
		t.Fatalf("Timeout wating for rebuild: %d", cachedbs.bloom.ElementsAdded())
	}

	cacheFails := 0
	cd.SetFunc(func() {
		cacheFails++
	})

	for i := 0; i < 1000; i++ {
		cachedbs.Has(blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i+2000))).Cid())
	}

	if float64(cacheFails)/float64(1000) > float64(0.05) {
		t.Fatal("Bloom filter has cache miss rate of more than 5%")
	}

	cacheFails = 0
	block := blocks.NewBlock([]byte("newBlock"))

	cachedbs.PutMany([]blocks.Block{block})
	if cacheFails != 2 {
		t.Fatalf("expected two datastore hits: %d", cacheFails)
	}
	cachedbs.Put(block)
	if cacheFails != 3 {
		t.Fatalf("expected datastore hit: %d", cacheFails)
	}

	if has, err := cachedbs.Has(block.Cid()); !has || err != nil {
		t.Fatal("has gave wrong response")
	}

	bl, err := cachedbs.Get(block.Cid())
	if bl.String() != block.String() {
		t.Fatal("block data doesn't match")
	}

	if err != nil {
		t.Fatal("there should't be an error")
	}
}

type callbackDatastore struct {
	sync.Mutex
	f  func()
	ds ds.Datastore
}

func (c *callbackDatastore) SetFunc(f func()) {
	c.Lock()
	defer c.Unlock()
	c.f = f
}

func (c *callbackDatastore) CallF() {
	c.Lock()
	defer c.Unlock()
	c.f()
}

func (c *callbackDatastore) Put(key ds.Key, value interface{}) (err error) {
	c.CallF()
	return c.ds.Put(key, value)
}

func (c *callbackDatastore) Get(key ds.Key) (value interface{}, err error) {
	c.CallF()
	return c.ds.Get(key)
}

func (c *callbackDatastore) Has(key ds.Key) (exists bool, err error) {
	c.CallF()
	return c.ds.Has(key)
}

func (c *callbackDatastore) Delete(key ds.Key) (err error) {
	c.CallF()
	return c.ds.Delete(key)
}

func (c *callbackDatastore) Query(q dsq.Query) (dsq.Results, error) {
	c.CallF()
	return c.ds.Query(q)
}

func (c *callbackDatastore) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(c), nil
}
