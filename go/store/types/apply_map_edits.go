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

// EmptyEditProvider is an EditProvider implementation that has no edits
type EmptyEditProvider struct{}

// Next will always return nil
func (eep EmptyEditProvider) Next() *KVP {
	return nil
}

// NumEdits will always return 0
func (eep EmptyEditProvider) NumEdits() int64 {
	return 0
}

// Before edits can be applied th cursor position for each edit must be found.  mapWork represents a piece of work to be
// done by the worker threads which are executing the prepWorker function.  Each piece of work will be a batch of edits
// whose cursor needs to be found, and a chan where results should be written.
type mapWork struct {
	resChan chan mapWorkResult
	kvps    []*KVP
}

// mapWorkResult is the result of a single mapWork instance being processed.
type mapWorkResult struct {
	seqCurs       []*sequenceCursor
	cursorEntries [][]mapEntry
}

const (
	maxWorkerCount = 7

	// batch sizes start small in order to get the sequenceChunker work to do quickly.  Batches will grow to a maximum
	// size at a given multiplier
	batchSizeStart = 10
	batchMult      = 1.25
	batchSizeMax   = 5000
)

// AppliedEditStats contains statistics on what edits were applied in types.ApplyEdits
type AppliedEditStats struct {
	// Additions counts the number of elements added to the map
	Additions int64

	// Modifications counts the number of map entries that were modified
	Modifications int64

	// SamVal counts the number of edits that had no impact because a value was set to the same value that is already
	// stored in the map
	SameVal int64

	// Deletions counts the number of items deleted from the map
	Deletions int64

	// NonexistantDeletes counts the number of items where a deletion was attempted, but the key didn't exist in the map
	// so there was no impact
	NonExistentDeletes int64
}

// Add adds two AppliedEditStats structures member by member.
func (stats AppliedEditStats) Add(other AppliedEditStats) AppliedEditStats {
	return AppliedEditStats{
		Additions:          stats.Additions + other.Additions,
		Modifications:      stats.Modifications + other.Modifications,
		SameVal:            stats.SameVal + other.SameVal,
		Deletions:          stats.Deletions + other.Deletions,
		NonExistentDeletes: stats.NonExistentDeletes + other.NonExistentDeletes,
	}
}

// ApplyEdits applies all the edits to a given Map and returns the resulting map, and some statistics about the edits
// that were applied.
func ApplyEdits(ctx context.Context, f *Format, edits EditProvider, m Map) (Map, AppliedEditStats) {
	var stats AppliedEditStats

	if edits.NumEdits() == 0 {
		return m, stats // no edits
	}

	seq := m.orderedSequence
	vrw := seq.valueReadWriter()

	numWorkers := int(edits.NumEdits()/batchSizeStart) + 1

	if numWorkers > maxWorkerCount {
		numWorkers = maxWorkerCount
	}

	rc := make(chan chan mapWorkResult, 128)
	wc := make(chan mapWork, 128)

	// start worker threads
	for i := 0; i < numWorkers; i++ {
		go prepWorker(ctx, seq, wc)
	}

	// asynchronously add mapWork to be done by the workers
	go buildBatches(f, rc, wc, edits)

	// wait for workers to return results and then process them
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
						stats.NonExistentDeletes++
						continue // already non-present
					}

					if existingValue != nil && kv.value != nil && existingValue.Equals(kv.value) {
						stats.SameVal++
						continue // same value
					}

					if ch == nil {
						ch = newSequenceChunker(ctx, cur, 0, vrw, makeMapLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(MapKind, vrw), mapHashValueBytes)
					} else {
						ch.advanceTo(ctx, cur)
					}

					if existingValue != nil {
						stats.Modifications++
						ch.Skip(ctx)
					} else {
						stats.Additions++
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
		return m, stats // no edits required application
	}

	return newMap(ch.Done(ctx).(orderedSequence)), stats
}

// prepWorker will wait for work to be read from a channel, then iterate over all of the edits finding the appropriate
// cursor where the insertion should happen.  It attempts to reuse cursors when consecutive keys share the same
// insertion point
func prepWorker(ctx context.Context, seq orderedSequence, wc chan mapWork) {
	for work := range wc {
		wRes := mapWorkResult{}

		var cur *sequenceCursor
		var curKey orderedKey

		i := 0
		for ; i < len(work.kvps); i++ {
			edit := work.kvps[i]
			key := edit.Key.Value(ctx)
			ordKey := newOrderedKey(key, seq.format())

			if cur == nil || !ordKey.Less(seq.format(), curKey) {
				cur = newCursorAt(ctx, seq, ordKey, true, false)

				if cur.valid() {
					curKey = getCurrentKey(cur)
				} else {
					break
				}
			}

			appendToWRes(ctx, &wRes, cur, key, edit.Val)
		}

		// All remaining edits get added at the end
		for ; i < len(work.kvps); i++ {
			edit := work.kvps[i]
			key := edit.Key.Value(ctx)
			appendToWRes(ctx, &wRes, cur, key, edit.Val)
		}

		work.resChan <- wRes
	}
}

// buildBatches iterates over the sorted edits building batches of work to be completed by the worker threads.
func buildBatches(f *Format, rc chan chan mapWorkResult, wc chan mapWork, edits EditProvider) {

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

			if nextEdit != nil && !edit.Key.Less(f, nextEdit.Key) {
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
