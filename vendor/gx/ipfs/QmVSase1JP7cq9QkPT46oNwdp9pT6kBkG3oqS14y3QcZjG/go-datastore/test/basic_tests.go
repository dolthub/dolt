package dstest

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	dstore "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

func SubtestBasicPutGet(t *testing.T, ds dstore.Datastore) {
	k := dstore.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := ds.Put(k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := ds.Has(k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := ds.Get(k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	outb, ok := out.([]byte)
	if !ok {
		t.Fatalf("output type wasnt []byte, it was %T", out)
	}

	if !bytes.Equal(outb, val) {
		t.Fatal("value received on get wasnt what we expected:", outb)
	}

	have, err = ds.Has(k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = ds.Delete(k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = ds.Has(k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
}

func SubtestNotFounds(t *testing.T, ds dstore.Datastore) {
	badk := dstore.NewKey("notreal")

	val, err := ds.Get(badk)
	if err != dstore.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := ds.Has(badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}
}

func SubtestManyKeysAndQuery(t *testing.T, ds dstore.Datastore) {
	var keys []dstore.Key
	var keystrs []string
	var values [][]byte
	count := 100
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		dsk := dstore.NewKey(s)
		keystrs = append(keystrs, dsk.String())
		keys = append(keys, dsk)
		buf := make([]byte, 64)
		rand.Read(buf)
		values = append(values, buf)
	}

	t.Logf("putting %d values", count)
	for i, k := range keys {
		err := ds.Put(k, values[i])
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, k := range keys {
		val, err := ds.Get(k)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		valb, ok := val.([]byte)
		if !ok {
			t.Fatalf("expected []byte as output from get, got: %T", val)
		}

		if !bytes.Equal(valb, values[i]) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	q := dsq.Query{KeysOnly: true}
	resp, err := ds.Query(q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	t.Log("aggregating query results")
	var outkeys []string
	for {
		res, ok := resp.NextSync()
		if res.Error != nil {
			t.Fatal("query result error: ", res.Error)
		}
		if !ok {
			break
		}

		outkeys = append(outkeys, res.Key)
	}

	t.Log("verifying query output")
	sort.Strings(keystrs)
	sort.Strings(outkeys)

	if len(keystrs) != len(outkeys) {
		t.Fatal("got wrong number of keys back")
	}

	for i, s := range keystrs {
		if outkeys[i] != s {
			t.Fatal("in key output, got %s but expected %s", outkeys[i], s)
		}
	}

	t.Log("deleting all keys")
	for _, k := range keys {
		if err := ds.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
}
