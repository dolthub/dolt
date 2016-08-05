// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type mapMutator struct {
	oc  *opCache
	vrw ValueReadWriter
}

func newMutator(vrw ValueReadWriter) *mapMutator {
	return &mapMutator{newOpCache(vrw), vrw}
}

func (mx *mapMutator) Set(key Value, val Value) *mapMutator {
	d.Chk.True(mx.oc != nil, "Can't call Set() again after Finish()")
	mx.oc.Set(key, val)
	return mx
}

func (mx *mapMutator) Finish() Map {
	d.Chk.True(mx.oc != nil, "Can only call Finish() once")
	defer func() {
		mx.oc.Destroy()
		mx.oc = nil
	}()

	seq := newEmptySequenceChunker(mx.vrw, mx.vrw, makeMapLeafChunkFn(mx.vrw), newOrderedMetaSequenceChunkFn(MapKind, mx.vrw), mapHashValueBytes)

	// I tried splitting this up so that the iteration ran in a separate goroutine from the Append'ing, but it actually made things a bit slower when I ran a test.
	iter := mx.oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		seq.Append(iter.Op())
	}
	return newMap(seq.Done().(orderedSequence))
}
