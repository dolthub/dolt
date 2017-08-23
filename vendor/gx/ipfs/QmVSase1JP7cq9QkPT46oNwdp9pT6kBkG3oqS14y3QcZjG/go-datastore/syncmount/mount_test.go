package syncmount_test

import (
	"testing"

	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	mount "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/syncmount"
)

func TestPutBadNothing(t *testing.T) {
	m := mount.New(nil)

	err := m.Put(datastore.NewKey("quux"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("Put got wrong error: %v != %v", g, e)
	}
}

func TestPutBadNoMount(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	err := m.Put(datastore.NewKey("/quux/thud"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("expected ErrNoMount, got: %v\n", g)
	}
}

func TestPut(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := m.Put(datastore.NewKey("/quux/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	val, err := mapds.Get(datastore.NewKey("/thud"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	buf, ok := val.([]byte)
	if !ok {
		t.Fatalf("Get value is not []byte: %T %v", val, val)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestGetBadNothing(t *testing.T) {
	m := mount.New([]mount.Mount{})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNoMount(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFound(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	_, err := m.Get(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGet(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	val, err := m.Get(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}

	buf, ok := val.([]byte)
	if !ok {
		t.Fatalf("Get value is not []byte: %T %v", val, val)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestHasBadNothing(t *testing.T) {
	m := mount.New([]mount.Mount{})

	found, err := m.Has(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasBadNoMount(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/redherring"), Datastore: mapds},
	})

	found, err := m.Has(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasNotFound(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	found, err := m.Has(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHas(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	found, err := m.Has(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestDeleteNotFound(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	err := m.Delete(datastore.NewKey("/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestDelete(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	if err := mapds.Put(datastore.NewKey("/thud"), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := m.Delete(datastore.NewKey("/quux/thud"))
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// make sure it disappeared
	found, err := mapds.Has(datastore.NewKey("/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestQuerySimple(t *testing.T) {
	mapds := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/quux"), Datastore: mapds},
	})

	const myKey = "/quux/thud"
	if err := m.Put(datastore.NewKey(myKey), []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	res, err := m.Query(query.Query{Prefix: "/quux"})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := false
	for _, e := range entries {
		switch e.Key {
		case datastore.NewKey(myKey).String():
			seen = true
		default:
			t.Errorf("saw unexpected key: %q", e.Key)
		}
	}
	if !seen {
		t.Errorf("did not see wanted key %q in %+v", myKey, entries)
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryCross(t *testing.T) {
	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()
	mapds2 := datastore.NewMapDatastore()
	mapds3 := datastore.NewMapDatastore()
	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/foo"), Datastore: mapds1},
		{Prefix: datastore.NewKey("/bar"), Datastore: mapds2},
		{Prefix: datastore.NewKey("/baz"), Datastore: mapds3},
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
	})

	m.Put(datastore.NewKey("/foo/lorem"), "123")
	m.Put(datastore.NewKey("/bar/ipsum"), "234")
	m.Put(datastore.NewKey("/bar/dolor"), "345")
	m.Put(datastore.NewKey("/baz/sit"), "456")
	m.Put(datastore.NewKey("/banana"), "567")

	res, err := m.Query(query.Query{Prefix: "/ba"})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := 0

	expect := map[string]string{
		"/foo/lorem": "y u here",
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
		"/baz/sit":   "456",
		"/banana":    "567",
	}
	for _, e := range entries {
		v := expect[e.Key]
		if v == "" {
			t.Errorf("unexpected key %s", e.Key)
		}

		if v != e.Value {
			t.Errorf("key value didn't match expected %s: '%s' - '%s'", e.Key, v, e.Value)
		}

		expect[e.Key] = "seen"
		seen++
	}

	if seen != 4 {
		t.Errorf("expected to see 3 values, saw %d", seen)
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestLookupPrio(t *testing.T) {
	mapds0 := datastore.NewMapDatastore()
	mapds1 := datastore.NewMapDatastore()

	m := mount.New([]mount.Mount{
		{Prefix: datastore.NewKey("/"), Datastore: mapds0},
		{Prefix: datastore.NewKey("/foo"), Datastore: mapds1},
	})

	m.Put(datastore.NewKey("/foo/bar"), "123")
	m.Put(datastore.NewKey("/baz"), "234")

	found, err := mapds0.Has(datastore.NewKey("/baz"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds0.Has(datastore.NewKey("/foo/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds1.Has(datastore.NewKey("/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}
