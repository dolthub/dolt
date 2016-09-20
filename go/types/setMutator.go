// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type setMutator struct {
	oc  opCache
	vrw ValueReadWriter
}

func newSetMutator(vrw ValueReadWriter) *setMutator {
	return &setMutator{vrw.opCache(), vrw}
}

func (mx *setMutator) Insert(val Value) *setMutator {
	d.PanicIfFalse(mx.oc != nil, "Can't call Insert() again after Finish()")
	mx.oc.GraphSetInsert(nil, val)
	return mx
}

func (mx *setMutator) Finish() Set {
	d.PanicIfFalse(mx.oc != nil, "Can only call Finish() once")
	defer func() {
		mx.oc = nil
	}()

	seq := newEmptySequenceChunker(mx.vrw, mx.vrw, makeSetLeafChunkFn(mx.vrw), newOrderedMetaSequenceChunkFn(SetKind, mx.vrw), hashValueBytes)

	// I tried splitting this up so that the iteration ran in a separate goroutine from the Append'ing, but it actually made things a bit slower when I ran a test.
	iter := mx.oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		keys, kind, item := iter.GraphOp()
		d.PanicIfFalse(0 == len(keys))
		d.PanicIfFalse(SetKind == kind)
		seq.Append(item)
	}
	return newSet(seq.Done().(orderedSequence))
}
