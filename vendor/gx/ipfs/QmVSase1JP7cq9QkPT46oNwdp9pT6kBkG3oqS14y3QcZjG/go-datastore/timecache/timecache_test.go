package timecache

import (
	"testing"
	"time"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

func testHas(t *testing.T, d ds.Datastore, k ds.Key, v interface{}) {
	if v2, err := d.Get(k); err != nil {
		t.Error(err)
	} else if v2 != v {
		t.Error("value incorrect", d, k, v, v2)
	}

	if has, err := d.Has(k); err != nil {
		t.Error(err)
	} else if !has {
		t.Error("should have it", d, k, v)
	}
}

func testNotHas(t *testing.T, d ds.Datastore, k ds.Key) {
	if _, err := d.Get(k); err == nil {
		t.Error("should not have it", d, k)
	}

	if has, err := d.Has(k); err != nil {
		t.Error(err)
	} else if has {
		t.Error("should not have it", d, k)
	}
}

func TestTimeCache(t *testing.T) {
	ttl := time.Millisecond * 100
	cache := WithTTL(ttl)
	cache.Put(ds.NewKey("foo1"), "bar1")
	cache.Put(ds.NewKey("foo2"), "bar2")

	<-time.After(ttl / 2)
	cache.Put(ds.NewKey("foo3"), "bar3")
	cache.Put(ds.NewKey("foo4"), "bar4")
	testHas(t, cache, ds.NewKey("foo1"), "bar1")
	testHas(t, cache, ds.NewKey("foo2"), "bar2")
	testHas(t, cache, ds.NewKey("foo3"), "bar3")
	testHas(t, cache, ds.NewKey("foo4"), "bar4")

	<-time.After(ttl / 2)
	testNotHas(t, cache, ds.NewKey("foo1"))
	testNotHas(t, cache, ds.NewKey("foo2"))
	testHas(t, cache, ds.NewKey("foo3"), "bar3")
	testHas(t, cache, ds.NewKey("foo4"), "bar4")

	cache.Delete(ds.NewKey("foo3"))
	testNotHas(t, cache, ds.NewKey("foo3"))

	<-time.After(ttl / 2)
	testNotHas(t, cache, ds.NewKey("foo1"))
	testNotHas(t, cache, ds.NewKey("foo2"))
	testNotHas(t, cache, ds.NewKey("foo3"))
	testNotHas(t, cache, ds.NewKey("foo4"))
}
