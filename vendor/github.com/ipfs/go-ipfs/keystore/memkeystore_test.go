package keystore

import (
	"sort"
	"testing"
)

func TestMemKeyStoreBasics(t *testing.T) {
	ks := NewMemKeystore()

	l, err := ks.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(l) != 0 {
		t.Fatal("expected no keys")
	}

	k1 := privKeyOrFatal(t)
	k2 := privKeyOrFatal(t)
	k3 := privKeyOrFatal(t)
	k4 := privKeyOrFatal(t)

	err = ks.Put("foo", k1)
	if err != nil {
		t.Fatal(err)
	}

	err = ks.Put("bar", k2)
	if err != nil {
		t.Fatal(err)
	}

	l, err = ks.List()
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(l)
	if l[0] != "bar" || l[1] != "foo" {
		t.Fatal("wrong entries listed")
	}

	err = ks.Put("foo", k3)
	if err == nil {
		t.Fatal("should not be able to overwrite key")
	}

	exist, err := ks.Has("foo")
	if !exist {
		t.Fatal("should know it has a key named foo")
	}
	if err != nil {
		t.Fatal(err)
	}

	exist, err = ks.Has("nonexistingkey")
	if exist {
		t.Fatal("should know it doesn't have a key named nonexistingkey")
	}
	if err != nil {
		t.Fatal(err)
	}

	if err := ks.Delete("bar"); err != nil {
		t.Fatal(err)
	}
	if err := ks.Put("beep", k3); err != nil {
		t.Fatal(err)
	}

	if err := ks.Put("boop", k4); err != nil {
		t.Fatal(err)
	}
	if err := assertGetKey(ks, "foo", k1); err != nil {
		t.Fatal(err)
	}

	if err := assertGetKey(ks, "beep", k3); err != nil {
		t.Fatal(err)
	}

	if err := assertGetKey(ks, "boop", k4); err != nil {
		t.Fatal(err)
	}

	if err := ks.Put("..///foo/", k1); err == nil {
		t.Fatal("shouldnt be able to put a poorly named key")
	}

	if err := ks.Put("", k1); err == nil {
		t.Fatal("shouldnt be able to put a key with no name")
	}

	if err := ks.Put(".foo", k1); err == nil {
		t.Fatal("shouldnt be able to put a key with a 'hidden' name")
	}
}
