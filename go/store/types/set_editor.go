// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/d"
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

func (se *SetEditor) Value(ctx context.Context) (Value, error) {
	return se.Set(ctx)
}

func (se *SetEditor) Set(ctx context.Context) (Set, error) {
	if len(se.edits.edits) == 0 {
		return se.s, nil // no edits
	}

	seq := se.s.orderedSequence
	vrw := seq.valueReadWriter()

	err := se.normalize(ctx)

	if err != nil {
		return EmptySet, err
	}

	ae := atomicerr.New()
	cursChan := make(chan chan *sequenceCursor)
	editChan := make(chan setEdit)

	go func() {
		defer close(cursChan)
		defer close(editChan)

		for i, edit := range se.edits.edits {
			if ae.IsSet() {
				break
			}

			if i+1 < len(se.edits.edits) && se.edits.edits[i+1].value.Equals(edit.value) {
				continue // next edit supersedes this one
			}

			edit := edit

			// Load cursor. TODO: Use ReadMany
			cc := make(chan *sequenceCursor, 1)
			cursChan <- cc

			go func() {
				cur, err := newCursorAtValue(ctx, seq, edit.value, true, false)

				if err != nil {
					ae.SetIfError(err)
					return
				}

				cc <- cur
			}()

			editChan <- edit
		}
	}()

	var ch *sequenceChunker
	for cc := range cursChan {
		if ae.IsSet() {
			//drain
			continue
		}

		cur := <-cc
		edit := <-editChan

		exists := false
		if cur.idx < cur.seq.seqLen() {
			item, err := cur.current()

			if ae.SetIfErrAndCheck(err) {
				continue
			}

			v := item.(Value)
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

		var err error
		if ch == nil {
			ch, err = newSequenceChunker(ctx, cur, 0, vrw, makeSetLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(SetKind, vrw), newMapChunker, hashValueBytes)
		} else {
			err = ch.advanceTo(ctx, cur)
		}

		if ae.SetIfErrAndCheck(err) {
			continue
		}

		if edit.insert {
			_, err = ch.Append(ctx, edit.value)
		} else {
			err = ch.Skip(ctx)
		}

		if ae.SetIfErrAndCheck(err) {
			continue
		}
	}

	if ae.IsSet() {
		return EmptySet, ae.Get()
	}

	if ch == nil {
		return se.s, nil // no edits required application
	}

	chSeq, err := ch.Done(ctx)

	if err != nil {
		return EmptySet, err
	}

	return newSet(chSeq.(orderedSequence)), nil
}

func (se *SetEditor) Insert(ctx context.Context, vs ...Value) (*SetEditor, error) {
	SortWithErroringLess(ctx, se.s.format(), ValueSort{vs})
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		err := se.edit(ctx, v, true)

		if err != nil {
			return nil, err
		}
	}
	return se, nil
}

func (se *SetEditor) Remove(ctx context.Context, vs ...Value) (*SetEditor, error) {
	SortWithErroringLess(ctx, se.s.format(), ValueSort{vs})
	for _, v := range vs {
		d.PanicIfTrue(v == nil)
		err := se.edit(ctx, v, false)

		if err != nil {
			return nil, err
		}
	}
	return se, nil
}

func (se *SetEditor) Has(ctx context.Context, v Value) (bool, error) {
	if idx, found, err := se.findEdit(ctx, v); err != nil {
		return false, err
	} else if found {
		return se.edits.edits[idx].insert, nil
	}

	return se.s.Has(ctx, v)
}

func (se *SetEditor) edit(ctx context.Context, v Value, insert bool) error {
	if len(se.edits.edits) == 0 {
		se.edits.edits = append(se.edits.edits, setEdit{v, insert})
		return nil
	}

	final := se.edits.edits[len(se.edits.edits)-1]
	if final.value.Equals(v) {
		se.edits.edits[len(se.edits.edits)-1] = setEdit{v, insert}
		return nil // update the last edit
	}

	se.edits.edits = append(se.edits.edits, setEdit{v, insert})

	isLess, err := final.value.Less(ctx, se.s.format(), v)

	if err != nil {
		return err
	}

	if se.normalized && isLess {
		// fast-path: edits take place in key-order
		return nil
	}

	// de-normalize
	se.normalized = false
	return nil
}

// Find the edit position of the last edit for a given key
func (se *SetEditor) findEdit(ctx context.Context, v Value) (int, bool, error) {
	err := se.normalize(ctx)

	if err != nil {
		return 0, false, err
	}

	var found bool
	idx, err := SearchWithErroringLess(len(se.edits.edits), func(i int) (bool, error) {
		return se.edits.edits[i].value.Less(ctx, se.s.format(), v)
	})

	if err != nil {
		return 0, false, err
	}

	if idx == len(se.edits.edits) {
		return idx, found, nil
	}

	if !se.edits.edits[idx].value.Equals(v) {
		return idx, found, nil
	}

	// advance to final edit position where kv.key == k
	for idx < len(se.edits.edits) && se.edits.edits[idx].value.Equals(v) {
		idx++
	}
	idx--

	found = true
	return idx, found, nil
}

func (se *SetEditor) normalize(ctx context.Context) error {
	if se.normalized {
		return nil
	}

	err := SortWithErroringLess(ctx, se.s.format(), se.edits)
	if err != nil {
		return err
	}

	// TODO: GC duplicate keys over some threshold of collectable memory?
	se.normalized = true
	return nil
}

type setEdit struct {
	value  Value
	insert bool
}

type setEditSlice struct {
	edits []setEdit
}

func (ses setEditSlice) Len() int      { return len(ses.edits) }
func (ses setEditSlice) Swap(i, j int) { ses.edits[i], ses.edits[j] = ses.edits[j], ses.edits[i] }
func (ses setEditSlice) Less(ctx context.Context, nbf *NomsBinFormat, i, j int) (bool, error) {
	return ses.edits[i].value.Less(ctx, nbf, ses.edits[j].value)
}
