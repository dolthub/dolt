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

package types

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/atomicerr"
)

// EditProvider is an interface which provides map edits as KVPs where each edit is a key and the new value
// associated with the key for inserts and updates.  deletes are modeled as a key with no value
type EditProvider interface {
	// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
	// in key sorted order.  Once all KVPs have been read io.EOF will be returned.
	Next(ctx context.Context) (*KVP, error)

	// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
	// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
	// been read
	ReachedEOF() bool

	Close(ctx context.Context) error
}

// EmptyEditProvider is an EditProvider implementation that has no edits
type EmptyEditProvider struct{}

// Next will always return nil, io.EOF
func (eep EmptyEditProvider) Next(ctx context.Context) (*KVP, error) {
	return nil, io.EOF
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (eep EmptyEditProvider) ReachedEOF() bool {
	return true
}

func (eep EmptyEditProvider) Close(ctx context.Context) error {
	return nil
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
	workerCount = 7

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

	// NonexistentDeletes counts the number of items where a deletion was attempted, but the key didn't exist in the map
	// so there was no impact
	NonExistentDeletes int64
}

// ApplyEdits applies all the edits to a given Map and returns the resulting map, and some statistics about the edits
// that were applied.
func ApplyEdits(ctx context.Context, edits EditProvider, m Map) (Map, error) {
	return ApplyNEdits(ctx, edits, m, -1)
}

func ApplyNEdits(ctx context.Context, edits EditProvider, m Map, numEdits int64) (Map, error) {
	if edits.ReachedEOF() {
		return m, nil // no edits
	}

	var seq sequence = m.orderedSequence
	vrw := seq.valueReadWriter()

	ae := atomicerr.New()
	rc := make(chan chan mapWorkResult, 128)
	wc := make(chan mapWork, 128)

	// start worker threads
	for i := 0; i < workerCount; i++ {
		go prepWorker(ctx, ae, seq.(orderedSequence), wc)
	}

	// asynchronously add mapWork to be done by the workers
	go buildBatches(ctx, m.valueReadWriter(), ae, rc, wc, edits, numEdits)

	// wait for workers to return results and then process them
	var ch *sequenceChunker
	for {
		wrc, ok := <-rc

		if ok {
			workRes, workResOk := <-wrc

			if !workResOk || ae.IsSet() {
				// drain
				continue
			}

			for i, cur := range workRes.seqCurs {
				for _, kv := range workRes.cursorEntries[i] {
					var existingValue Value
					if cur.idx < cur.seq.seqLen() {
						currEnt, err := cur.current()

						if ae.SetIfErrAndCheck(err) {
							continue
						}

						ckv := currEnt.(mapEntry)
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
						var err error
						ch, err = newMapLeafChunkerFromCursor(ctx, cur, vrw)

						if ae.SetIfError(err) {
							continue
						}
					} else {
						err := ch.advanceTo(ctx, cur)

						if ae.SetIfError(err) {
							continue
						}
					}

					if existingValue != nil {
						err := ch.Skip(ctx)

						if ae.SetIfError(err) {
							continue
						}
					}

					if kv.value != nil {
						_, err := ch.Append(ctx, kv)

						if ae.SetIfError(err) {
							continue
						}
					}
				}
			}
		} else {
			break
		}
	}

	if ae.IsSet() {
		return EmptyMap, ae.Get()
	}

	if ch == nil {
		return m, nil // no edits required application
	}

	seq, err := ch.Done(ctx)

	if err != nil {
		return EmptyMap, err
	}

	return newMap(seq.(orderedSequence)), nil
}

// prepWorker will wait for work to be read from a channel, then iterate over all of the edits finding the appropriate
// cursor where the insertion should happen.  It attempts to reuse cursors when consecutive keys share the same
// insertion point
func prepWorker(ctx context.Context, ae *atomicerr.AtomicError, seq orderedSequence, wc chan mapWork) {
	for work := range wc {
		// In the case of an error drain wc
		if !ae.IsSet() {
			wRes, err := doWork(ctx, seq, work)

			if err != nil {
				ae.SetIfError(err)
			} else {
				work.resChan <- wRes
			}
		}

		close(work.resChan)
	}
}

func doWork(ctx context.Context, seq orderedSequence, work mapWork) (mapWorkResult, error) {
	wRes := mapWorkResult{}

	var cur *sequenceCursor
	var curKey orderedKey

	i := 0
	for ; i < len(work.kvps); i++ {
		edit := work.kvps[i]

		key, err := edit.Key.Value(ctx)

		if err != nil {
			return mapWorkResult{}, err
		}

		ordKey, err := newOrderedKey(key, seq.format())

		if err != nil {
			return mapWorkResult{}, err
		}

		createCur := cur == nil
		if cur != nil {
			isLess, err := ordKey.Less(ctx, seq.format(), curKey)

			if err != nil {
				return mapWorkResult{}, err
			}

			createCur = !isLess
		}

		if createCur {
			cur, err = newCursorAt(ctx, seq, ordKey, true, false)

			if err != nil {
				return mapWorkResult{}, err
			}

			if cur.valid() {
				curKey, err = getCurrentKey(cur)

				if err != nil {
					return mapWorkResult{}, err
				}
			} else {
				break
			}
		}

		err = appendToWRes(ctx, &wRes, cur, key, edit.Val)

		if err != nil {
			return mapWorkResult{}, err
		}
	}

	// All remaining edits get added at the end
	for ; i < len(work.kvps); i++ {
		edit := work.kvps[i]
		key, err := edit.Key.Value(ctx)

		if err != nil {
			return mapWorkResult{}, err
		}

		err = appendToWRes(ctx, &wRes, cur, key, edit.Val)

		if err != nil {
			return mapWorkResult{}, err
		}
	}

	return wRes, nil
}

// buildBatches iterates over the sorted edits building batches of work to be completed by the worker threads.
func buildBatches(ctx context.Context, vr ValueReader, ae *atomicerr.AtomicError, rc chan chan mapWorkResult, wc chan mapWork, edits EditProvider, numEdits int64) {
	defer close(rc)
	defer close(wc)

	batchSize := batchSizeStart
	nextEdit, err := edits.Next(ctx)

	if err == io.EOF {
		return
	} else if ae.SetIfError(err) {
		return
	}

	var editsRead int64
	for {
		batch := make([]*KVP, 0, batchSize)

		for len(batch) < batchSize {
			edit := nextEdit

			nextEdit, err = edits.Next(ctx)
			if err == io.EOF {
				if edit != nil {
					batch = append(batch, edit)
				}
				break
			} else if ae.SetIfError(err) {
				return
			}

			isLess, err := edit.Key.Less(ctx, vr.Format(), nextEdit.Key)

			if ae.SetIfError(err) {
				return
			}

			// keys are sorted, so if this key is not less than the next key then they are equal and the next
			// value will take precedence
			if !isLess {
				continue
			}

			batch = append(batch, edit)
			editsRead++

			if editsRead == numEdits {
				break
			}
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
}

func appendToWRes(ctx context.Context, wRes *mapWorkResult, cur *sequenceCursor, key Value, val Valuable) error {
	var mEnt mapEntry
	if val == nil {
		mEnt = mapEntry{key, nil}
	} else if v, ok := val.(Value); ok {
		mEnt = mapEntry{key, v}
	} else {
		sv, err := val.Value(ctx)

		if err != nil {
			return err
		}

		mEnt = mapEntry{key, sv}
	}

	wRes.seqCurs = append(wRes.seqCurs, cur)
	wRes.cursorEntries = append(wRes.cursorEntries, []mapEntry{mEnt})

	return nil
}

func newMapLeafChunkerFromCursor(ctx context.Context, cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error) {
	makeChunk := makeMapLeafChunkFn(vrw)
	makeParentChunk := newOrderedMetaSequenceChunkFn(MapKind, vrw)
	return newSequenceChunker(ctx, cur, 0, vrw, makeChunk, makeParentChunk, newMapChunker, mapHashValueBytes)
}
