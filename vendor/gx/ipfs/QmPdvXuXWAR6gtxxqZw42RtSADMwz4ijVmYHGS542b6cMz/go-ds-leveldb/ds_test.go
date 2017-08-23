package leveldb

import (
	"io/ioutil"
	"os"
	"testing"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//  d, close := newDS(t)
//  defer close()
func newDS(t *testing.T) (*datastore, func()) {
	path, err := ioutil.TempDir("/tmp", "testing_leveldb_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		os.RemoveAll(path)
		d.Close()
	}
}

// newDSMem returns an in-memory datastore.
func newDSMem(t *testing.T) *datastore {
	d, err := NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func addTestCases(t *testing.T, d *datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		v2b := v2.([]byte)
		if string(v2b) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}

}

func testQuery(t *testing.T, d *datastore) {
	addTestCases(t, d, testcases)

	rs, err := d.Query(dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)
}

func TestQuery(t *testing.T) {
	d, close := newDS(t)
	defer close()
	testQuery(t, d)
}
func TestQueryMem(t *testing.T) {
	d := newDSMem(t)
	testQuery(t, d)
}

func TestQueryRespectsProcess(t *testing.T) {
	d, close := newDS(t)
	defer close()
	addTestCases(t, d, testcases)
}

func TestQueryRespectsProcessMem(t *testing.T) {
	d := newDSMem(t)
	addTestCases(t, d, testcases)
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func testBatching(t *testing.T, d *datastore) {
	b, err := d.Batch()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val.([]byte)) {
			t.Fatal("got wrong data!")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()
	testBatching(t, d)
}

func TestBatchingMem(t *testing.T) {
	d := newDSMem(t)
	testBatching(t, d)
}
