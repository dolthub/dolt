package tiered

import (
	"testing"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dscb "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/callback"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

func testHas(t *testing.T, dses []ds.Datastore, k ds.Key, v interface{}) {
	// all under should have it
	for _, d := range dses {
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
}

func testNotHas(t *testing.T, dses []ds.Datastore, k ds.Key) {
	// all under should not have it
	for _, d := range dses {
		if _, err := d.Get(k); err == nil {
			t.Error("should not have it", d, k)
		}

		if has, err := d.Has(k); err != nil {
			t.Error(err)
		} else if has {
			t.Error("should not have it", d, k)
		}
	}
}

func TestTiered(t *testing.T) {
	d1 := ds.NewMapDatastore()
	d2 := ds.NewMapDatastore()
	d3 := ds.NewMapDatastore()
	d4 := ds.NewMapDatastore()

	td := New(d1, d2, d3, d4)
	td.Put(ds.NewKey("foo"), "bar")
	testHas(t, []ds.Datastore{td}, ds.NewKey("foo"), "bar")
	testHas(t, td, ds.NewKey("foo"), "bar") // all children

	// remove it from, say, caches.
	d1.Delete(ds.NewKey("foo"))
	d2.Delete(ds.NewKey("foo"))
	testHas(t, []ds.Datastore{td}, ds.NewKey("foo"), "bar")
	testHas(t, td[2:], ds.NewKey("foo"), "bar")
	testNotHas(t, td[:2], ds.NewKey("foo"))

	// write it again.
	td.Put(ds.NewKey("foo"), "bar2")
	testHas(t, []ds.Datastore{td}, ds.NewKey("foo"), "bar2")
	testHas(t, td, ds.NewKey("foo"), "bar2")
}

func TestQueryCallsLast(t *testing.T) {
	var d1n, d2n, d3n int
	d1 := dscb.Wrap(ds.NewMapDatastore(), func() { d1n++ })
	d2 := dscb.Wrap(ds.NewMapDatastore(), func() { d2n++ })
	d3 := dscb.Wrap(ds.NewMapDatastore(), func() { d3n++ })

	td := New(d1, d2, d3)

	td.Query(dsq.Query{})
	if d3n < 1 {
		t.Error("should call last")
	}
}
