// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/attic-labs/noms/go/d"
)

// MapEditor allows for efficient editing of Map-typed prolly trees. Edits
// are buffered to memory and can be applied via Build(), which returns a new
// Map. Prior to Build(), Get() & Has() will return the value that the resulting
// Map would return if it were built immediately prior to the respective call.
// Note: The implementation biases performance towards a usage which applies
// edits in key-order.
type MapEditor struct {
	m   Map
	ase *AsyncSortedEdits
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, NewAsyncSortedEdits(10000, 2, 4)}
}

func (me *MapEditor) Map(ctx context.Context) Map {
	me.ase.FinishedEditing()
	me.ase.Sort()

	if me.ase.Size() == 0 {
		return me.m // no edits
	}

	seq := me.m.orderedSequence
	vrw := seq.valueReadWriter()

	cursChan := make(chan chan *sequenceCursor)
	kvsChan := make(chan chan mapEntry)

	go func() {
		itr := me.ase.Iterator()
		for i, edit := 0, itr.Next(); edit != nil; i, edit = i+1, itr.Next() {
			nextEdit := itr.Peek()
			if nextEdit != nil && nextEdit.Key.Equals(edit.Key) {
				continue // next edit supercedes this one
			}

			edit := edit

			// TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				cc <- newCursorAtValue(ctx, seq, edit.Key, true, false)
			}()

			kvc := make(chan mapEntry, 1)
			kvsChan <- kvc

			if edit.Val == nil {
				kvc <- mapEntry{edit.Key, nil}
				continue
			}

			if v, ok := edit.Val.(Value); ok {
				kvc <- mapEntry{edit.Key, v}
				continue
			}

			go func() {
				sv := edit.Val.Value(ctx)
				if e, ok := sv.(Emptyable); ok {
					if e.Empty() {
						sv = nil
					}
				}

				kvc <- mapEntry{edit.Key, sv}
			}()
		}

		close(cursChan)
		close(kvsChan)
	}()

	var ch *sequenceChunker
	for cc := range cursChan {
		cur := <-cc
		kv := <-<-kvsChan

		var existingValue Value
		if cur.idx < cur.seq.seqLen() {
			ckv := cur.current().(mapEntry)
			if ckv.key.Equals(kv.key) {
				existingValue = ckv.value
			}
		}

		if existingValue == nil && kv.value == nil {
			continue // already non-present
		}

		if existingValue != nil && kv.value != nil && existingValue.Equals(kv.value) {
			continue // same value
		}

		if ch == nil {
			ch = newSequenceChunker(ctx, cur, 0, vrw, makeMapLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(MapKind, vrw), mapHashValueBytes)
		} else {
			ch.advanceTo(ctx, cur)
		}

		if existingValue != nil {
			ch.Skip(ctx)
		}
		if kv.value != nil {
			ch.Append(ctx, kv)
		}
	}

	if ch == nil {
		return me.m // no edits required application
	}

	return newMap(ch.Done(ctx).(orderedSequence))
}

func (me *MapEditor) Set(k Value, v Valuable) *MapEditor {
	d.PanicIfTrue(v == nil)
	me.set(k, v)
	return me
}

func (me *MapEditor) SetM(kv ...Valuable) *MapEditor {
	d.PanicIfFalse(len(kv)%2 == 0)

	for i := 0; i < len(kv); i += 2 {
		me.Set(kv[i].(Value), kv[i+1])
	}
	return me
}

func (me *MapEditor) Remove(k Value) *MapEditor {
	me.set(k, nil)
	return me
}

func (me *MapEditor) set(k Value, v Valuable) {
	me.ase.Set(k, v)
}
