// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type mapMutator struct {
	oc  opCache
	vrw ValueReadWriter
}

func newMapMutator(vrw ValueReadWriter) *mapMutator {
	return &mapMutator{vrw.opCache(), vrw}
}

func (mx *mapMutator) Set(key Value, val Value) *mapMutator {
	if mx.oc == nil {
		d.Panic("Can't call Set() again after Finish()")
	}
	mx.oc.GraphMapSet(nil, key, val)
	return mx
}

func (mx *mapMutator) Finish() Map {
	if mx.oc == nil {
		d.Panic("Can only call Finish() once")
	}
	defer func() {
		mx.oc = nil
	}()

	seq := newEmptySequenceChunker(mx.vrw, mx.vrw, makeMapLeafChunkFn(mx.vrw), newOrderedMetaSequenceChunkFn(MapKind, mx.vrw), mapHashValueBytes)

	// I tried splitting this up so that the iteration ran in a separate goroutine from the Append'ing, but it actually made things a bit slower when I ran a test.
	iter := mx.oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		keys, kind, item := iter.GraphOp()
		d.PanicIfFalse(0 == len(keys))
		d.PanicIfFalse(MapKind == kind)
		seq.Append(item)
	}
	return newMap(seq.Done().(orderedSequence))
}
