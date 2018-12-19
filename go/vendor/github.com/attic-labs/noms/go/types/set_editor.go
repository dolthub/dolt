// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

// SetEditor allows for efficient editing of Set-typed prolly trees. Edits
// are buffered to memory and can be applied via Build(), which returns a new
// Set. Prior to Build(), Get() & Has() will return the value that the resulting
// Set would return if it were built immediately prior to the respective call.
// Note: The implementation biases performance towards a usage which applies
// edits in key-order.
type SetEditor struct {
	s          Set
	edits      setEditSlice // edits may contain duplicate values, in which case, the last edit of a given key is used
	normalized bool
}

func NewSetEditor(s Set) *SetEditor {
	return &SetEditor{s, setEditSlice{}, true}
}

func (se *SetEditor) Kind() NomsKind {
	return SetKind
}

func (se *SetEditor) Value() Value {
	return se.Set()
}

func (se *SetEditor) Set() Set {
	if len(se.edits) == 0 {
		return se.s // no edits
	}

	seq := se.s.orderedSequence
	vrw := seq.valueReadWriter()

	se.normalize()

	cursChan := make(chan chan *sequenceCursor)
	editChan := make(chan setEdit)

	go func() {
		for i, edit := range se.edits {
			if i+1 < len(se.edits) && se.edits[i+1].value.Equals(edit.value) {
				continue // next edit supercedes this one
			}

			edit := edit

			// Load cursor. TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				cc <- newCursorAtValue(seq, edit.value, true, false)
			}()

			editChan <- edit
		}
		close(cursChan)
		close(editChan)
	}()

	var ch *sequenceChunker
	for cc := range cursChan {
		cur := <-cc
		edit := <-editChan

		exists := false
		if cur.idx < cur.seq.seqLen() {
			v := cur.current().(Value)
			if v.Equals(edit.value) {
				exists = true
			}
		}

		if exists && edit.insert {
			continue // already present
		}

		if !exists && !edit.insert {
			continue // already non-present
		}

		if ch == nil {
			ch = newSequenceChunker(cur, 0, vrw, makeSetLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(SetKind, vrw), hashValueBytes)
		} else {
			ch.advanceTo(cur)
		}

		if edit.insert {
			ch.Append(edit.value)
		} else {
			ch.Skip()
		}
	}

	if ch == nil {
		return se.s // no edits required application
	}

	return newSet(ch.Done().(orderedSequence))
}

func (se *SetEditor) Insert(vs ...Value) *SetEditor {
	sort.Stable(ValueSlice(vs))
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		se.edit(v, true)
	}
	return se
}

func (se *SetEditor) Remove(vs ...Value) *SetEditor {
	sort.Stable(ValueSlice(vs))
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		se.edit(v, false)
	}
	return se
}

func (se *SetEditor) Has(v Value) bool {
	if idx, found := se.findEdit(v); found {
		return se.edits[idx].insert
	}

	return se.s.Has(v)
}

func (se *SetEditor) edit(v Value, insert bool) {
	if len(se.edits) == 0 {
		se.edits = append(se.edits, setEdit{v, insert})
		return
	}

	final := se.edits[len(se.edits)-1]
	if final.value.Equals(v) {
		se.edits[len(se.edits)-1] = setEdit{v, insert}
		return // update the last edit
	}

	se.edits = append(se.edits, setEdit{v, insert})

	if se.normalized && final.value.Less(v) {
		// fast-path: edits take place in key-order
		return
	}

	// de-normalize
	se.normalized = false
}

// Find the edit position of the last edit for a given key
func (se *SetEditor) findEdit(v Value) (idx int, found bool) {
	se.normalize()

	idx = sort.Search(len(se.edits), func(i int) bool {
		return !se.edits[i].value.Less(v)
	})

	if idx == len(se.edits) {
		return
	}

	if !se.edits[idx].value.Equals(v) {
		return
	}

	// advance to final edit position where kv.key == k
	for idx < len(se.edits) && se.edits[idx].value.Equals(v) {
		idx++
	}
	idx--

	found = true
	return
}

func (se *SetEditor) normalize() {
	if se.normalized {
		return
	}

	sort.Stable(se.edits)
	// TODO: GC duplicate keys over some threshold of collectable memory?
	se.normalized = true
}

type setEdit struct {
	value  Value
	insert bool
}

type setEditSlice []setEdit

func (ses setEditSlice) Len() int           { return len(ses) }
func (ses setEditSlice) Swap(i, j int)      { ses[i], ses[j] = ses[j], ses[i] }
func (ses setEditSlice) Less(i, j int) bool { return ses[i].value.Less(ses[j].value) }
