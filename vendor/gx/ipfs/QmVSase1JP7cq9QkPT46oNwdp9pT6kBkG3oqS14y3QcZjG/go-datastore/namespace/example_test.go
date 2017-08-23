package namespace_test

import (
	"fmt"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	nsds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/namespace"
)

func Example() {
	mp := ds.NewMapDatastore()
	ns := nsds.Wrap(mp, ds.NewKey("/foo/bar"))

	k := ds.NewKey("/beep")
	v := "boop"

	ns.Put(k, v)
	fmt.Printf("ns.Put %s %s\n", k, v)

	v2, _ := ns.Get(k)
	fmt.Printf("ns.Get %s -> %s\n", k, v2)

	k3 := ds.NewKey("/foo/bar/beep")
	v3, _ := mp.Get(k3)
	fmt.Printf("mp.Get %s -> %s\n", k3, v3)
	// Output:
	// ns.Put /beep boop
	// ns.Get /beep -> boop
	// mp.Get /foo/bar/beep -> boop
}
