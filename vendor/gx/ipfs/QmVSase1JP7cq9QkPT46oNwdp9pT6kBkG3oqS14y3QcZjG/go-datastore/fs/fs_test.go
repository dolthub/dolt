package fs_test

import (
	"bytes"
	"testing"

	. "launchpad.net/gocheck"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	fs "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/fs"
	query "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DSSuite struct {
	dir string
	ds  ds.Datastore
}

var _ = Suite(&DSSuite{})

func (ks *DSSuite) SetUpTest(c *C) {
	ks.dir = c.MkDir()
	ks.ds, _ = fs.NewDatastore(ks.dir)
}

func (ks *DSSuite) TestOpen(c *C) {
	_, err := fs.NewDatastore("/tmp/foo/bar/baz")
	c.Assert(err, Not(Equals), nil)

	// setup ds
	_, err = fs.NewDatastore(ks.dir)
	c.Assert(err, Equals, nil)
}

func (ks *DSSuite) TestBasic(c *C) {

	keys := strsToKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ks.ds.Put(k, []byte(k.String()))
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v, err := ks.ds.Get(k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v.([]byte), []byte(k.String())), Equals, true)
	}

	r, err := ks.ds.Query(query.Query{Prefix: "/foo/bar/"})
	if err != nil {
		c.Check(err, Equals, nil)
	}

	expect := []string{
		"/foo/bar/baz",
		"/foo/bar/bazb",
		"/foo/bar/baz/barb",
	}
	all, err := r.Rest()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(all), Equals, len(expect))

	for _, k := range expect {
		found := false
		for _, e := range all {
			if e.Key == k {
				found = true
			}
		}

		if !found {
			c.Error("did not find expected key: ", k)
		}
	}
}

func strsToKeys(strs []string) []ds.Key {
	keys := make([]ds.Key, len(strs))
	for i, s := range strs {
		keys[i] = ds.NewKey(s)
	}
	return keys
}
