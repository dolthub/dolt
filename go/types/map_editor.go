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

type mapWorkResult struct {
	seqCur *sequenceCursor
	entry  mapEntry
}

type mapWork struct {
	resChan chan mapWorkResult
	kvp     *KVP
}

func (me *MapEditor) Map(ctx context.Context) Map {
	me.ase.FinishedEditing()
	me.ase.Sort()

	if me.ase.Size() == 0 {
		return me.m // no edits
	}

	seq := me.m.orderedSequence
	vrw := seq.valueReadWriter()

	numWorkers := 8
	rc := make(chan chan mapWorkResult, 128)
	wc := make(chan mapWork, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			for work := range wc {
				edit := work.kvp
				key := edit.Key.Value(ctx)
				cur := newCursorAtValue(ctx, seq, key, true, false)

				var mEnt mapEntry
				if edit.Val == nil {
					mEnt = mapEntry{key, nil}
				} else if v, ok := edit.Val.(Value); ok {
					mEnt = mapEntry{key, v}
				} else {
					sv := edit.Val.Value(ctx)
					mEnt = mapEntry{key, sv}
				}

				work.resChan <- mapWorkResult{cur, mEnt}

			}
		}()
	}

	go func() {
		itr := me.ase.Iterator()
		nextEdit := itr.Next()

		for {
			edit := nextEdit

			if edit == nil {
				break
			}

			nextEdit = itr.Next()

			if nextEdit != nil && !edit.Key.Less(nextEdit.Key) {
				// keys are sorted, so if this key is not less than the next key then they are equal and the next
				// value will take precedence
				continue
			}

			workResChan := make(chan mapWorkResult)
			work := mapWork{workResChan, edit}
			rc <- workResChan
			wc <- work

		}

		close(rc)
		close(wc)
	}()

	var ch *sequenceChunker
	for wrc := range rc {
		workRes := <-wrc

		cur := workRes.seqCur
		kv := workRes.entry

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

func (me *MapEditor) Set(k LesserValuable, v Valuable) *MapEditor {
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

func (me *MapEditor) Remove(k LesserValuable) *MapEditor {
	me.set(k, nil)
	return me
}

func (me *MapEditor) set(k LesserValuable, v Valuable) {
	me.ase.Set(k, v)
}
