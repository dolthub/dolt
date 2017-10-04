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
	edits      mapEditSlice // edits may contain duplicate key values, in which case, the last edit of a given key is used
	normalized bool
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, mapEditSlice{}, true}
}

func (me *MapEditor) Kind() NomsKind {
	return MapKind
}

func (me *MapEditor) Value() Value {
	return me.Map()
}

func (me *MapEditor) Map() Map {
	if len(me.edits) == 0 {
		return me.m // no edits
	}

	seq := me.m.orderedSequence
	vrw := seq.valueReadWriter()

	me.normalize()

	cursChan := make(chan chan *sequenceCursor)
	kvsChan := make(chan chan mapEntry)

	go func() {
		for i, edit := range me.edits {
			if i+1 < len(me.edits) && me.edits[i+1].key.Equals(edit.key) {
				continue // next edit supercedes this one
			}

			edit := edit

			// TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				cc <- newCursorAtValue(seq, edit.key, true, false)
			}()

			kvc := make(chan mapEntry, 1)
			kvsChan <- kvc

			if edit.value == nil {
				kvc <- mapEntry{edit.key, nil}
				continue
			}

			if v, ok := edit.value.(Value); ok {
				kvc <- mapEntry{edit.key, v}
				continue
			}

			go func() {
				sv := edit.value.Value()
				if e, ok := sv.(Emptyable); ok {
					if e.Empty() {
						sv = nil
					}
				}

				kvc <- mapEntry{edit.key, sv}
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
			ch = newSequenceChunker(cur, 0, vrw, makeMapLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(MapKind, vrw), mapHashValueBytes)
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

func (me *MapEditor) Get(k Value) Valuable {
	if idx, found := me.findEdit(k); found {
		v := me.edits[idx].value
		if v != nil {
			return v
		}
	}

	return me.m.Get(k)
}

func (me *MapEditor) Has(k Value) bool {
	if idx, found := me.findEdit(k); found {
		return me.edits[idx].value != nil
	}

	return me.m.Has(k)
}

func (me *MapEditor) set(k Value, v Valuable) {
	if len(me.edits) == 0 {
		me.edits = append(me.edits, mapEdit{k, v})
		return
	}

	final := me.edits[len(me.edits)-1]
	if final.key.Equals(k) {
		me.edits[len(me.edits)-1] = mapEdit{k, v}
		return // update the last edit
	}

	me.edits = append(me.edits, mapEdit{k, v})

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

type mapEdit struct {
	key   Value
	value Valuable
}

type mapEditSlice []mapEdit

func (mes mapEditSlice) Len() int           { return len(mes) }
func (mes mapEditSlice) Swap(i, j int)      { mes[i], mes[j] = mes[j], mes[i] }
func (mes mapEditSlice) Less(i, j int) bool { return mes[i].key.Less(mes[j].key) }
