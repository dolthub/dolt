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
	seqCurs       []*sequenceCursor
	cursorEntries [][]mapEntry
}

type mapWork struct {
	resChan chan mapWorkResult
	kvps    []*KVP
}

func (me *MapEditor) Map(ctx context.Context) Map {
	me.ase.FinishedEditing()
	me.ase.Sort()

	if me.ase.Size() == 0 {
		return me.m // no edits
	}

	seq := me.m.orderedSequence
	vrw := seq.valueReadWriter()

	numWorkers := 7
	rc := make(chan chan mapWorkResult, 128)
	wc := make(chan mapWork, 128)

	for i := 0; i < numWorkers; i++ {
		go func() {
			for work := range wc {
				wRes := mapWorkResult{}

				var cur *sequenceCursor
				var curKey orderedKey

				i := 0
				for ; i < len(work.kvps); i++ {
					edit := work.kvps[i]
					key := edit.Key.Value(ctx)
					ordKey := newOrderedKey(key)

					if cur == nil || !ordKey.Less(curKey) {
						cur = newCursorAt(ctx, seq, ordKey, true, false)

						if cur.valid() {
							curKey = getCurrentKey(cur)
						} else {
							break
						}
					}

					appendToWRes(ctx, &wRes, cur, key, edit.Val)
				}

				for ; i < len(work.kvps); i++ {
					edit := work.kvps[i]
					key := edit.Key.Value(ctx)
					appendToWRes(ctx, &wRes, cur, key, edit.Val)
				}

				work.resChan <- wRes
			}
		}()
	}

	const batchSizeMax = 5000
	const batchSizeStart = 10
	const batchMult = 1.25

	go func() {
		batchSize := batchSizeStart
		itr := me.ase.Iterator()
		nextEdit := itr.Next()

		for {
			batch := make([]*KVP, 0, batchSize)

			for i := 0; i < batchSize; i++ {
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

				batch = append(batch, edit)
			}

			if len(batch) > 0 {
				workResChan := make(chan mapWorkResult)
				work := mapWork{workResChan, batch}
				rc <- workResChan
				wc <- work
			} else {
				break
			}

			batchSize = int(float32(batchSize) * batchMult)
			if batchSize > batchSizeMax {
				batchSize = batchSizeMax
			}
		}

		close(rc)
		close(wc)
	}()

	//var waitTime time.Duration
	var ch *sequenceChunker
	for {
		//start := time.Now()
		wrc, ok := <-rc
		//waitTime += time.Now().Sub(start)

		if ok {
			//start = time.Now()
			workRes := <-wrc
			//waitTime += time.Now().Sub(start)

			for i, cur := range workRes.seqCurs {
				for _, kv := range workRes.cursorEntries[i] {
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
			}
		} else {
			break
		}
	}

	//fmt.Printf("Total time spent waiting %f ms\n", (1000.0 * waitTime.Seconds()))

	if ch == nil {
		return me.m // no edits required application
	}

	return newMap(ch.Done(ctx).(orderedSequence))
}

func appendToWRes(ctx context.Context, wRes *mapWorkResult, cur *sequenceCursor, key Value, val Valuable) {
	var mEnt mapEntry
	if val == nil {
		mEnt = mapEntry{key, nil}
	} else if v, ok := val.(Value); ok {
		mEnt = mapEntry{key, v}
	} else {
		sv := val.Value(ctx)
		mEnt = mapEntry{key, sv}
	}

	wRes.seqCurs = append(wRes.seqCurs, cur)
	wRes.cursorEntries = append(wRes.cursorEntries, []mapEntry{mEnt})
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

func (me *MapEditor) EditCount() int64 {
	return me.ase.Size()
}
