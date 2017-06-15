// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

// MapEditor allows for efficient editing of Map-typed prolly trees. Edits
// are buffered to memory and can be applied via Build(), which returns a new
// Map. Prior to Build(), Get() & Has() will return the value that the resulting
// Map would return if it were built immediately prior to the respective call.
// Note: The implementation biases performance towards a usage which applies
// edits in key-order.
type MapEditor struct {
	m          Map
	edits      mapEntrySlice // edits may contain duplicate key values, in which case, the last edit of a given key is used
	normalized bool
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, mapEntrySlice{}, true}
}

func (me *MapEditor) Build(vrw ValueReadWriter) Map {
	if len(me.edits) == 0 {
		return me.m // no edits
	}

	vr := me.m.sequence().valueReader()

	me.normalize()

	var ch *sequenceChunker
	for i, kv := range me.edits {
		if i+1 < len(me.edits) && me.edits[i+1].key.Equals(kv.key) {
			continue // next edit supercedes this one
		}

		// TODO: Parallelize loading of cursors
		cur := newCursorAtValue(me.m.seq, kv.key, true, false, false)

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
			ch = newSequenceChunker(cur, 0, vr, vrw, makeMapLeafChunkFn(vr), newOrderedMetaSequenceChunkFn(MapKind, vr), mapHashValueBytes)
		} else {
			ch.advanceTo(cur)
		}

		if existingValue != nil {
			ch.Skip()
		}
		if kv.value != nil {
			ch.Append(kv)
		}
	}

	if ch == nil {
		return me.m // no edits required application
	}

	return newMap(ch.Done().(orderedSequence))
}

func (me *MapEditor) Set(k, v Value) *MapEditor {
	d.PanicIfTrue(v == nil)
	me.set(k, v)
	return me
}

func (me *MapEditor) SetM(kv ...Value) *MapEditor {
	d.PanicIfFalse(len(kv)%2 == 0)

	for i := 0; i < len(kv); i += 2 {
		me.Set(kv[i], kv[i+1])
	}
	return me
}

func (me *MapEditor) Remove(k Value) *MapEditor {
	me.set(k, nil)
	return me
}

func (me *MapEditor) Get(k Value) Value {
	if idx, found := me.findEdit(k); found {
		return me.edits[idx].value
	}

	return me.m.Get(k)
}

func (me *MapEditor) Has(k Value) bool {
	if idx, found := me.findEdit(k); found {
		return me.edits[idx].value != nil
	}

	return me.m.Has(k)
}

func (me *MapEditor) set(k, v Value) {
	if len(me.edits) == 0 {
		me.edits = append(me.edits, mapEntry{k, v})
		return
	}

	final := me.edits[len(me.edits)-1]
	if final.key.Equals(k) {
		me.edits[len(me.edits)-1] = mapEntry{k, v}
		return // update the last edit
	}

	me.edits = append(me.edits, mapEntry{k, v})

	if me.normalized && final.key.Less(k) {
		// fast-path: edits take place in key-order
		return
	}

	// de-normalize
	me.normalized = false
}

// Find the edit position of the last edit for a given key
func (me *MapEditor) findEdit(k Value) (idx int, found bool) {
	me.normalize()

	idx = sort.Search(len(me.edits), func(i int) bool {
		return !me.edits[i].key.Less(k)
	})

	if idx == len(me.edits) {
		return
	}

	if !me.edits[idx].key.Equals(k) {
		return
	}

	// advance to final edit position where kv.key == k
	for idx < len(me.edits) && me.edits[idx].key.Equals(k) {
		idx++
	}
	idx--

	found = true
	return
}

func (me *MapEditor) normalize() {
	if me.normalized {
		return
	}

	sort.Stable(me.edits)
	// TODO: GC duplicate keys over some threshold of collectable memory?
	me.normalized = true
}
