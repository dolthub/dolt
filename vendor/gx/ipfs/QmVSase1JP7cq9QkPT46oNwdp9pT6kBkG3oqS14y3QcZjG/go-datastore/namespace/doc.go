// Package namespace introduces a namespace Datastore Shim, which basically
// mounts the entire child datastore under a prefix.
//
// Use the Wrap function to wrap a datastore with any Key prefix. For example:
//
//  import (
//    "fmt"
//
//    ds "github.com/ipfs/go-datastore"
//    nsds "github.com/ipfs/go-datastore/namespace"
//  )
//
//  func main() {
//    mp := ds.NewMapDatastore()
//    ns := nsds.Wrap(mp, ds.NewKey("/foo/bar"))
//
//    // in the Namespace Datastore:
//    ns.Put(ds.NewKey("/beep"), "boop")
//    v2, _ := ns.Get(ds.NewKey("/beep")) // v2 == "boop"
//
//    // and, in the underlying MapDatastore:
//    v3, _ := mp.Get(ds.NewKey("/foo/bar/beep")) // v3 == "boop"
//  }
package namespace
