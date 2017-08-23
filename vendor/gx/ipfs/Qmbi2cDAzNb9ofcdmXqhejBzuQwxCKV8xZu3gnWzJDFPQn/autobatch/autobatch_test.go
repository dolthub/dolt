package autobatch

import (
	"fmt"
	"testing"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

func TestBasicPuts(t *testing.T) {
	d := NewAutoBatching(ds.NewMapDatastore(), 16)

	k := ds.NewKey("test")
	v := "hello world"

	err := d.Put(k, v)
	if err != nil {
		t.Fatal(err)
	}

	out, err := d.Get(k)
	if err != nil {
		t.Fatal(err)
	}

	if out != v {
		t.Fatal("wasnt the same! ITS NOT THE SAME")
	}
}

func TestFlushing(t *testing.T) {
	child := ds.NewMapDatastore()
	d := NewAutoBatching(child, 16)

	var keys []ds.Key
	for i := 0; i < 16; i++ {
		keys = append(keys, ds.NewKey(fmt.Sprintf("test%d", i)))
	}
	v := "hello world"

	for _, k := range keys {
		err := d.Put(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := child.Get(keys[0])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	err = d.Put(ds.NewKey("test16"), v)
	if err != nil {
		t.Fatal(err)
	}

	// should be flushed now, try to get keys from child datastore
	for _, k := range keys {
		val, err := child.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if val != v {
			t.Fatal(err)
		}
	}
}
