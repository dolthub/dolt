// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type ListEditor struct {
	l     List
	edits *listEdit
}

func NewListEditor(l List) *ListEditor {
	return &ListEditor{l, nil}
}

func (le *ListEditor) Kind() NomsKind {
	return ListKind
}

func (le *ListEditor) Value(ctx context.Context) (Value, error) {
	return le.List(ctx)
}

func (le *ListEditor) List(ctx context.Context) (List, error) {
	if le.edits == nil {
		return le.l, nil // no edits
	}

	seq := le.l.sequence
	vrw := seq.valueReadWriter()

	ae := nbs.NewAtomicError()
	cursChan := make(chan chan *sequenceCursor)
	spliceChan := make(chan chan listEdit)

	go func() {
		defer close(cursChan)
		defer close(spliceChan)

		for edit := le.edits; edit != nil; edit = edit.next {
			if ae.IsSet() {
				break
			}

			edit := edit

			// TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				defer close(cc)

				cur, err := newCursorAtIndex(ctx, seq, edit.idx)

				if ae.SetIfError(err) {
					return
				}
				cc <- cur
			}()

			sc := make(chan listEdit, 1)
			spliceChan <- sc

			wg := sync.WaitGroup{}
			subEditors := false
			for i, v := range edit.inserted {
				if _, ok := v.(Value); ok {
					continue
				}

				subEditors = true
				idx, val := i, v
				wg.Add(1)
				go func() {
					defer wg.Done()

					var err error
					edit.inserted[idx], err = val.Value(ctx)
					ae.SetIfError(err)
				}()
			}

			if !subEditors {
				sc <- *edit
				continue
			}

			go func() {
				wg.Wait()
				sc <- *edit
			}()
		}
	}()

	var ch *sequenceChunker
	for cc := range cursChan {
		cur, ok := <-cc

		if !ok || ae.IsSet() {
			continue // drain
		}

		if cur == nil {
			return EmptyList, ae.Get()
		}

		sp, ok := <-<-spliceChan

		if !ok || ae.IsSet() {
			continue // drain
		}

		var err error
		if ch == nil {
			ch, err = newSequenceChunker(ctx, cur, 0, vrw, makeListLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw), hashValueBytes)
		} else {
			err = ch.advanceTo(ctx, cur)
		}

		if ae.SetIfError(err) {
			continue
		}

		dc := sp.removed
		for dc > 0 {
			ch.Skip(ctx)
			dc--
		}

		for _, v := range sp.inserted {
			if emp, ok := v.(Emptyable); ok && emp.Empty() {
				continue
			}

			_, err := ch.Append(ctx, v)

			if ae.SetIfError(err) {
				break
			}
		}
	}

	if err := ae.Get(); err != nil {
		return EmptyList, err
	}

	seq, err := ch.Done(ctx)

	if err != nil {
		return EmptyList, err
	}

	return newList(seq), nil
}

func collapseListEdit(newEdit, edit *listEdit) bool {
	if newEdit.idx+newEdit.removed < edit.idx ||
		edit.idx+uint64(len(edit.inserted)) < newEdit.idx {
		return false
	}

	collapsed := &listEdit{}

	if newEdit.idx <= edit.idx {
		collapsed.idx = newEdit.idx

		overlap := newEdit.removed - (edit.idx - newEdit.idx) // number of leading N values removed from edit.inserted
		if overlap < uint64(len(edit.inserted)) {
			// newEdit doesn't remove all of edit.inserted
			collapsed.inserted = append(newEdit.inserted, edit.inserted[overlap:]...)
			collapsed.removed = newEdit.removed + edit.removed - overlap
		} else {
			// newEdit removes all of edit.inserted
			collapsed.inserted = newEdit.inserted
			collapsed.removed = newEdit.removed + edit.removed - uint64(len(edit.inserted))
		}
	} else {
		// edit.idx < newEdit.idx

		collapsed.idx = edit.idx

		editInsertedLen := uint64(len(edit.inserted))
		beginEditRemovePoint := newEdit.idx - edit.idx

		if beginEditRemovePoint == editInsertedLen {
			// newEdit took place at the position immediately after the last element of edit.inserted
			collapsed.inserted = append(edit.inserted, newEdit.inserted...)
			collapsed.removed = edit.removed + newEdit.removed
		} else {
			// newEdit takes place within edit.inserted
			collapsed.inserted = append(collapsed.inserted, edit.inserted[:beginEditRemovePoint]...)
			collapsed.inserted = append(collapsed.inserted, newEdit.inserted...)

			endEditRemovePoint := beginEditRemovePoint + newEdit.removed
			if endEditRemovePoint < editInsertedLen {
				// elements of edit.inserted remain beyond newEdit.removed
				collapsed.removed = edit.removed
				collapsed.inserted = append(collapsed.inserted, edit.inserted[endEditRemovePoint:]...)
			} else {
				collapsed.removed = edit.removed + endEditRemovePoint - editInsertedLen
			}
		}
	}

	*newEdit = *collapsed
	return true
}

func (le *ListEditor) Len() uint64 {
	delta := int64(0)
	for edit := le.edits; edit != nil; edit = edit.next {
		delta += -int64(edit.removed) + int64(len(edit.inserted))
	}

	return uint64(int64(le.l.Len()) + delta)
}

func (le *ListEditor) Splice(idx uint64, deleteCount uint64, vs ...Valuable) *ListEditor {
	for _, sv := range vs {
		d.PanicIfTrue(sv == nil)
	}

	ne := &listEdit{idx, deleteCount, vs, nil}

	var last *listEdit
	edit := le.edits

	for edit != nil {
		if collapseListEdit(ne, edit) {
			if last == nil {
				le.edits = edit.next
			} else {
				last.next = edit.next
			}

			edit = edit.next
			continue
		}

		if edit.idx > ne.idx {
			break
		}

		ne.idx = adjustIdx(ne.idx, edit)
		last = edit
		edit = edit.next
	}

	if ne.removed == 0 && len(ne.inserted) == 0 {
		return le // effectively removed 1 or more existing slices
	}

	if ne.idx > le.l.Len() {
		d.Panic("Index Out Of Bounds")
	}
	if ne.idx == le.l.Len() && ne.removed > 0 {
		d.Panic("Index Out Of Bounds")
	}

	if last == nil {
		// Insert |ne| in first position
		ne.next = le.edits
		le.edits = ne
	} else {
		ne.next = last.next
		last.next = ne
	}

	return le
}

func (le *ListEditor) Set(idx uint64, v Valuable) *ListEditor {
	return le.Splice(idx, 1, v)
}

func (le *ListEditor) Append(vs ...Valuable) *ListEditor {
	return le.Splice(le.Len(), 0, vs...)
}

func (le *ListEditor) Insert(idx uint64, vs ...Valuable) *ListEditor {
	return le.Splice(idx, 0, vs...)
}

func (le *ListEditor) Remove(start uint64, end uint64) *ListEditor {
	d.PanicIfFalse(start <= end)
	return le.Splice(start, end-start)
}

func (le *ListEditor) RemoveAt(idx uint64) *ListEditor {
	return le.Splice(idx, 1)
}

func adjustIdx(idx uint64, e *listEdit) uint64 {
	return idx + e.removed - uint64(len(e.inserted))
}

func (le *ListEditor) Get(ctx context.Context, idx uint64) (Valuable, error) {
	edit := le.edits
	for edit != nil {
		if edit.idx > idx {
			// idx is before next splice
			return le.l.Get(ctx, idx)
		}

		if edit.idx <= idx && idx < (edit.idx+uint64(len(edit.inserted))) {
			// idx is within the insert values of edit
			return edit.inserted[idx-edit.idx], nil
		}

		idx = adjustIdx(idx, edit)
		edit = edit.next
	}

	return le.l.Get(ctx, idx)
}

type listEdit struct {
	idx      uint64
	removed  uint64
	inserted []Valuable
	next     *listEdit
}
