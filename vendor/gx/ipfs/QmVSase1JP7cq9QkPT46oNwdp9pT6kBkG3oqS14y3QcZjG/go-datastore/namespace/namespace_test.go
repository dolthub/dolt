package namespace_test

import (
	"bytes"
	"sort"
	"testing"

	. "launchpad.net/gocheck"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	ns "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/namespace"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DSSuite struct{}

var _ = Suite(&DSSuite{})

func (ks *DSSuite) TestBasic(c *C) {
	ks.testBasic(c, "abc")
	ks.testBasic(c, "")
}

func (ks *DSSuite) testBasic(c *C, prefix string) {

	mpds := ds.NewMapDatastore()
	nsds := ns.Wrap(mpds, ds.NewKey(prefix))

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := nsds.Put(k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v1, err := nsds.Get(k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1.([]byte), []byte(k.String())), Equals, true)

		v2, err := mpds.Get(ds.NewKey(prefix).Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v2.([]byte), []byte(k.String())), Equals, true)
	}

	run := func(d ds.Datastore, q dsq.Query) []ds.Key {
		r, err := d.Query(q)
		c.Check(err, Equals, nil)

		e, err := r.Rest()
		c.Check(err, Equals, nil)

		return ds.EntryKeys(e)
	}

	listA := run(mpds, dsq.Query{})
	listB := run(nsds, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))

	// sort them cause yeah.
	sort.Sort(ds.KeySlice(listA))
	sort.Sort(ds.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		c.Check(nsds.InvertKey(kA), Equals, kB)
		c.Check(kA, Equals, nsds.ConvertKey(kB))
	}
}

func (ks *DSSuite) TestQuery(c *C) {
	mpds := ds.NewMapDatastore()
	nsds := ns.Wrap(mpds, ds.NewKey("/foo"))

	keys := strsToKeys([]string{
		"abc/foo",
		"bar/foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/baz/abc",
		"xyz/foo",
	})

	for _, k := range keys {
		err := mpds.Put(k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	qres, err := nsds.Query(dsq.Query{})
	c.Check(err, Equals, nil)

	expect := []dsq.Entry{
		{Key: "/bar", Value: []byte("/foo/bar")},
		{Key: "/bar/baz", Value: []byte("/foo/bar/baz")},
		{Key: "/baz/abc", Value: []byte("/foo/baz/abc")},
	}

	results, err := qres.Rest()
	c.Check(err, Equals, nil)

	for i, ent := range results {
		c.Check(ent.Key, Equals, expect[i].Key)
		entval, _ := ent.Value.([]byte)
		expval, _ := expect[i].Value.([]byte)
		c.Check(string(entval), Equals, string(expval))
	}

	err = qres.Close()
	c.Check(err, Equals, nil)

	qres, err = nsds.Query(dsq.Query{Prefix: "bar"})
	c.Check(err, Equals, nil)

	expect = []dsq.Entry{
		{Key: "/bar", Value: []byte("/foo/bar")},
		{Key: "/bar/baz", Value: []byte("/foo/bar/baz")},
	}

	results, err = qres.Rest()
	c.Check(err, Equals, nil)

	for i, ent := range results {
		c.Check(ent.Key, Equals, expect[i].Key)
		entval, _ := ent.Value.([]byte)
		expval, _ := expect[i].Value.([]byte)
		c.Check(string(entval), Equals, string(expval))
	}
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}
