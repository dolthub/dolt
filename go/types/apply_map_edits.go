package types

import (
	"context"
)

// EditProvider is an interface which provides map edits as KVPs where each edit is a key and the new value
// associated with the key for inserts and updates.  deletes are modeled as a key with no value
type EditProvider interface {
	// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
	// in key sorted order
	Next() *KVP

	// NumEdits returns the number of KVPs representing the edits that will be provided when calling next
	NumEdits() int64
}

type EmptyEditProvider struct{}

func (eep EmptyEditProvider) Next() *KVP {
	return nil
}

func (eep EmptyEditProvider) NumEdits() int64 {
	return 0
}

type mapWorkResult struct {
	seqCurs       []*sequenceCursor
	cursorEntries [][]mapEntry
}

type mapWork struct {
	resChan chan mapWorkResult
	kvps    []*KVP
}

func prepWorker(ctx context.Context, seq orderedSequence, wc chan mapWork) {
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
}

func buildBatches(rc chan chan mapWorkResult, wc chan mapWork, edits EditProvider) {
	const batchSizeMax = 5000
	const batchSizeStart = 10
	const batchMult = 1.25

	batchSize := batchSizeStart
	nextEdit := edits.Next()

	for {
		batch := make([]*KVP, 0, batchSize)

		for i := 0; i < batchSize; i++ {
			edit := nextEdit

			if edit == nil {
				break
			}

			nextEdit = edits.Next()

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
}

func ApplyEdits(ctx context.Context, edits EditProvider, m Map) Map {
	if edits.NumEdits() == 0 {
		return m // no edits
	}

	seq := m.orderedSequence
	vrw := seq.valueReadWriter()

	numWorkers := 7
	rc := make(chan chan mapWorkResult, 128)
	wc := make(chan mapWork, 128)

	for i := 0; i < numWorkers; i++ {
		go prepWorker(ctx, seq, wc)
	}

	go buildBatches(rc, wc, edits)

	var ch *sequenceChunker
	for {
		wrc, ok := <-rc

		if ok {
			workRes := <-wrc

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

	if ch == nil {
		return m // no edits required application
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
