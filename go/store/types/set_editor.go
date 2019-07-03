// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
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

func (se *SetEditor) Value(ctx context.Context) Value {
	return se.Set(ctx)
}

func (se *SetEditor) Set(ctx context.Context) Set {
	if len(se.edits.edits) == 0 {
		return se.s // no edits
	}

	seq := se.s.orderedSequence
	vrw := seq.valueReadWriter()

	se.normalize()

	cursChan := make(chan chan *sequenceCursor)
	editChan := make(chan setEdit)

	go func() {
		for i, edit := range se.edits.edits {
			if i+1 < len(se.edits.edits) && se.edits.edits[i+1].value.Equals(edit.value) {
				continue // next edit supercedes this one
			}

			edit := edit

			// Load cursor. TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				cc <- newCursorAtValue(ctx, se.s.format, seq, edit.value, true, false)
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
			ch = newSequenceChunker(ctx, cur, 0, vrw, makeSetLeafChunkFn(se.s.format, vrw), newOrderedMetaSequenceChunkFn(SetKind, se.s.format, vrw), hashValueBytes)
		} else {
			ch.advanceTo(ctx, cur)
		}

		if edit.insert {
			ch.Append(ctx, edit.value)
		} else {
			ch.Skip(ctx)
		}
	}

	if ch == nil {
		return se.s // no edits required application
	}

	return newSet(se.s.format, ch.Done(ctx).(orderedSequence))
}

func (se *SetEditor) Insert(vs ...Value) *SetEditor {
	sort.Stable(ValueSort{vs, se.s.format})
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		se.edit(v, true)
	}
	return se
}

func (se *SetEditor) Remove(vs ...Value) *SetEditor {
	sort.Stable(ValueSort{vs, se.s.format})
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		se.edit(v, false)
	}
	return se
}

func (se *SetEditor) Has(ctx context.Context, v Value) bool {
	if idx, found := se.findEdit(v); found {
		return se.edits.edits[idx].insert
	}

	return se.s.Has(ctx, v)
}

func (se *SetEditor) edit(v Value, insert bool) {
	if len(se.edits.edits) == 0 {
		se.edits.edits = append(se.edits.edits, setEdit{v, insert})
		return
	}

	final := se.edits.edits[len(se.edits.edits)-1]
	if final.value.Equals(v) {
		se.edits.edits[len(se.edits.edits)-1] = setEdit{v, insert}
		return // update the last edit
	}

	se.edits.edits = append(se.edits.edits, setEdit{v, insert})

	if se.normalized && final.value.Less(se.s.format, v) {
		// fast-path: edits take place in key-order
		return
	}

	// de-normalize
	se.normalized = false
}

// Find the edit position of the last edit for a given key
func (se *SetEditor) findEdit(v Value) (idx int, found bool) {
	se.normalize()

	idx = sort.Search(len(se.edits.edits), func(i int) bool {
		return !se.edits.edits[i].value.Less(se.s.format, v)
	})

	if idx == len(se.edits.edits) {
		return
	}

	if !se.edits.edits[idx].value.Equals(v) {
		return
	}

	// advance to final edit position where kv.key == k
	for idx < len(se.edits.edits) && se.edits.edits[idx].value.Equals(v) {
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

type setEditSlice struct {
	edits  []setEdit
	format *Format
}

func (ses setEditSlice) Len() int      { return len(ses.edits) }
func (ses setEditSlice) Swap(i, j int) { ses.edits[i], ses.edits[j] = ses.edits[j], ses.edits[i] }
func (ses setEditSlice) Less(i, j int) bool {
	return ses.edits[i].value.Less(ses.format, ses.edits[j].value)
}
