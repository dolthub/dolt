// Package keytransform introduces a Datastore Shim that transforms keys before
// passing them to its child. It can be used to manipulate what keys look like
// to the user, for example namespacing keys, reversing them, etc.
//
// Use the Wrap function to wrap a datastore with any KeyTransform.
// A KeyTransform is simply an interface with two functions, a conversion and
// its inverse. For example:
//
//   import (
//     ktds "github.com/ipfs/go-datastore/keytransform"
//     ds "github.com/ipfs/go-datastore"
//   )
//
//   func reverseKey(k ds.Key) ds.Key {
//     return k.Reverse()
//   }
//
//   func invertKeys(d ds.Datastore) {
//     return ktds.Wrap(d, &ktds.Pair{
//       Convert: reverseKey,
//       Invert: reverseKey,  // reverse is its own inverse.
//     })
//   }
//
package keytransform
