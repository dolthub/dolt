package wantlist

import (
	"testing"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
)

var testcids []*cid.Cid

func init() {
	strs := []string{
		"QmQL8LqkEgYXaDHdNYCG2mmpow7Sp8Z8Kt3QS688vyBeC7",
		"QmcBDsdjgSXU7BP4A4V8LJCXENE5xVwnhrhRGVTJr9YCVj",
		"QmQakgd2wDxc3uUF4orGdEm28zUT9Mmimp5pyPG2SFS9Gj",
	}
	for _, s := range strs {
		c, err := cid.Decode(s)
		if err != nil {
			panic(err)
		}
		testcids = append(testcids, c)
	}

}

type wli interface {
	Contains(*cid.Cid) (*Entry, bool)
}

func assertHasCid(t *testing.T, w wli, c *cid.Cid) {
	e, ok := w.Contains(c)
	if !ok {
		t.Fatal("expected to have ", c)
	}
	if !e.Cid.Equals(c) {
		t.Fatal("returned entry had wrong cid value")
	}
}

func assertNotHasCid(t *testing.T, w wli, c *cid.Cid) {
	_, ok := w.Contains(c)
	if ok {
		t.Fatal("expected not to have ", c)
	}
}

func TestBasicWantlist(t *testing.T) {
	wl := New()

	if !wl.Add(testcids[0], 5) {
		t.Fatal("expected true")
	}
	assertHasCid(t, wl, testcids[0])
	if !wl.Add(testcids[1], 4) {
		t.Fatal("expected true")
	}
	assertHasCid(t, wl, testcids[0])
	assertHasCid(t, wl, testcids[1])

	if wl.Len() != 2 {
		t.Fatal("should have had two items")
	}

	if wl.Add(testcids[1], 4) {
		t.Fatal("add shouldnt report success on second add")
	}
	assertHasCid(t, wl, testcids[0])
	assertHasCid(t, wl, testcids[1])

	if wl.Len() != 2 {
		t.Fatal("should have had two items")
	}

	if !wl.Remove(testcids[0]) {
		t.Fatal("should have gotten true")
	}

	assertHasCid(t, wl, testcids[1])
	if _, has := wl.Contains(testcids[0]); has {
		t.Fatal("shouldnt have this cid")
	}
}

func TestSesRefWantlist(t *testing.T) {
	wl := NewThreadSafe()

	if !wl.Add(testcids[0], 5, 1) {
		t.Fatal("should have added")
	}
	assertHasCid(t, wl, testcids[0])
	if wl.Remove(testcids[0], 2) {
		t.Fatal("shouldnt have removed")
	}
	assertHasCid(t, wl, testcids[0])
	if wl.Add(testcids[0], 5, 1) {
		t.Fatal("shouldnt have added")
	}
	assertHasCid(t, wl, testcids[0])
	if !wl.Remove(testcids[0], 1) {
		t.Fatal("should have removed")
	}
	assertNotHasCid(t, wl, testcids[0])
}
