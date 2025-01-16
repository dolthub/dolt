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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

// type ValueInRange func(Value) (bool, error)
type ValueInRange func(context.Context, Value) (bool, bool, error)

var ErrKeysNotOrdered = errors.New("streaming map keys not ordered")

var EmptyMap Map

type Map struct {
	orderedSequence
}

func newMap(seq orderedSequence) Map {
	return Map{seq}
}

func mapHashValueBytes(item sequenceItem, sp sequenceSplitter) error {
	entry := item.(mapEntry)
	err := hashValueBytes(entry.key, sp)

	if err != nil {
		return err
	}

	err = hashValueBytes(entry.value, sp)

	if err != nil {
		return err
	}

	return nil
}

func newMapChunker(nbf *NomsBinFormat, salt byte) sequenceSplitter {
	return newRollingValueHasher(nbf, salt)
}

func NewMap(ctx context.Context, vrw ValueReadWriter, kv ...Value) (Map, error) {
	entries, err := buildMapData(ctx, vrw, kv)

	if err != nil {
		return EmptyMap, err
	}

	ch, err := newEmptyMapSequenceChunker(ctx, vrw)

	if err != nil {
		return EmptyMap, err
	}

	for _, entry := range entries.entries {
		_, err := ch.Append(ctx, entry)

		if err != nil {
			return EmptyMap, err
		}
	}

	seq, err := ch.Done(ctx)

	if err != nil {
		return EmptyMap, err
	}

	return newMap(seq.(orderedSequence)), nil
}

// NewStreamingMap takes an input channel of values and returns a value that
// will produce a finished Map when |.Wait()| is called.  Values sent to the
// input channel must be alternating keys and values. (e.g.  k1, v1, k2,
// v2...). Moreover keys need to be added to the channel in Noms sortorder,
// adding key values to the input channel out of order will result in an error.
// Once the input channel is closed by the caller, a finished Map will be
// available from the |Wait| call.
//
// See graph_builder.go for building collections with values that are not in
// order.
func NewStreamingMap(ctx context.Context, vrw ValueReadWriter, kvs <-chan Value) *StreamingMap {
	d.PanicIfTrue(vrw == nil)
	sm := &StreamingMap{}
	sm.eg, sm.egCtx = errgroup.WithContext(ctx)
	sm.eg.Go(func() error {
		m, err := readMapInput(sm.egCtx, vrw, kvs)
		sm.m = m
		return err
	})
	return sm
}

type StreamingMap struct {
	eg    *errgroup.Group
	egCtx context.Context
	m     Map
}

func (sm *StreamingMap) Wait() (Map, error) {
	err := sm.eg.Wait()
	return sm.m, err
}

// Done returns a signal channel which is closed once the StreamingMap is no
// longer reading from the key/values channel. A send to the key/value channel
// should be in a select with a read from this channel to ensure that the send
// does not deadlock.
func (sm *StreamingMap) Done() <-chan struct{} {
	return sm.egCtx.Done()
}

func readMapInput(ctx context.Context, vrw ValueReadWriter, kvs <-chan Value) (Map, error) {
	ch, err := newEmptyMapSequenceChunker(ctx, vrw)
	if err != nil {
		return EmptyMap, err
	}

	var lastK Value
	nextIsKey := true
	var k Value
LOOP:
	for {
		select {
		case v, ok := <-kvs:
			if !ok {
				break LOOP
			}
			if nextIsKey {
				k = v

				if lastK != nil {
					isLess, err := lastK.Less(ctx, vrw.Format(), k)
					if err != nil {
						return EmptyMap, err
					}
					if !isLess {
						return EmptyMap, ErrKeysNotOrdered
					}
				}
				lastK = k
				nextIsKey = false
			} else {
				_, err := ch.Append(ctx, mapEntry{key: k, value: v})
				if err != nil {
					return EmptyMap, err
				}

				nextIsKey = true
			}
		case <-ctx.Done():
			return EmptyMap, ctx.Err()
		}
	}

	seq, err := ch.Done(ctx)
	if err != nil {
		return EmptyMap, err
	}

	return newMap(seq.(orderedSequence)), nil
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (m Map) Diff(ctx context.Context, last Map, changes chan<- ValueChanged) error {
	if m.Equals(last) {
		return nil
	}
	return orderedSequenceDiffLeftRight(ctx, last.orderedSequence, m.orderedSequence, changes)
}

// DiffLeftRight computes the diff from |last| to |m| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (m Map) DiffLeftRight(ctx context.Context, last Map, changes chan<- ValueChanged) error {
	trueFunc := func(context.Context, Value) (bool, bool, error) {
		return true, false, nil
	}
	return m.DiffLeftRightInRange(ctx, last, nil, trueFunc, changes)
}

func (m Map) DiffLeftRightInRange(ctx context.Context, last Map, start Value, inRange ValueInRange, changes chan<- ValueChanged) error {
	if m.Equals(last) {
		return nil
	}

	startKey := emptyKey
	if !IsNull(start) {
		var err error
		startKey, err = newOrderedKey(start, m.Format())

		if err != nil {
			return err
		}
	}

	return orderedSequenceDiffLeftRightInRange(ctx, last.orderedSequence, m.orderedSequence, startKey, inRange, changes)
}

// Collection interface

func (m Map) asSequence() sequence {
	return m.orderedSequence
}

// Value interface
func (m Map) Value(ctx context.Context) (Value, error) {
	return m, nil
}

func (m Map) firstOrLast(ctx context.Context, last bool) (Value, Value, error) {
	cur, err := newCursorAt(ctx, m.orderedSequence, emptyKey, false, last)

	if err != nil {
		return nil, nil, err
	}

	if !cur.valid() {
		return nil, nil, nil
	}

	currItem, err := cur.current()

	if err != nil {
		return nil, nil, err
	}

	entry := currItem.(mapEntry)
	return entry.key, entry.value, nil
}

func (m Map) Empty() bool {
	if m.orderedSequence == nil {
		return true
	}

	return m.orderedSequence.Empty()
}

func (m Map) Format() *NomsBinFormat {
	return m.format()
}

func (m Map) First(ctx context.Context) (Value, Value, error) {
	return m.firstOrLast(ctx, false)
}

func (m Map) Last(ctx context.Context) (Value, Value, error) {
	return m.firstOrLast(ctx, true)
}

func (m Map) At(ctx context.Context, idx uint64) (key, value Value, err error) {
	if idx >= m.Len() {
		panic(fmt.Errorf("out of bounds: %d >= %d", idx, m.Len()))
	}

	cur, err := newSequenceIteratorAtIndex(ctx, m.orderedSequence, idx)

	if err != nil {
		return nil, nil, err
	}

	item, err := cur.current()

	if err != nil {
		return nil, nil, err
	}

	entry := item.(mapEntry)
	return entry.key, entry.value, nil
}

func (m Map) MaybeGet(ctx context.Context, key Value) (v Value, ok bool, err error) {
	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)

	if err != nil {
		return nil, false, err
	}

	if !cur.valid() {
		return nil, false, nil
	}

	item, err := cur.current()

	if err != nil {
		return nil, false, err
	}

	entry := item.(mapEntry)

	if !entry.key.Equals(key) {
		return nil, false, nil
	}

	return entry.value, true, nil
}

func (m Map) MaybeGetTuple(ctx context.Context, key Tuple) (v Tuple, ok bool, err error) {
	var val Value
	val, ok, err = m.MaybeGet(ctx, key)

	if val != nil {
		return val.(Tuple), ok, err
	}

	return Tuple{}, ok, err
}

func (m Map) Has(ctx context.Context, key Value) (bool, error) {
	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)

	if err != nil {
		return false, err
	}

	if !cur.valid() {
		return false, nil
	}

	item, err := cur.current()

	if err != nil {
		return false, err
	}

	entry := item.(mapEntry)
	return entry.key.Equals(key), nil
}

type mapIterCallback func(key, value Value) (stop bool, err error)

func (m Map) Iter(ctx context.Context, cb mapIterCallback) error {
	cur, err := newCursorAt(ctx, m.orderedSequence, emptyKey, false, false)

	if err != nil {
		return err
	}

	return cur.iter(ctx, func(v interface{}) (bool, error) {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

// Any returns true if cb() return true for any of the items in the map.
func (m Map) Any(ctx context.Context, cb func(k, v Value) bool) (yep bool, err error) {
	err = m.Iter(ctx, func(k, v Value) (bool, error) {
		if cb(k, v) {
			yep = true
			return true, nil
		}
		return false, nil
	})

	return yep, err
}

func (m Map) isPrimitive() bool {
	return false
}

func (m Map) Iterator(ctx context.Context) (MapIterator, error) {
	return m.IteratorAt(ctx, 0)
}

func (m Map) IteratorAt(ctx context.Context, pos uint64) (MapIterator, error) {
	cur, err := newSequenceIteratorAtIndex(ctx, m.orderedSequence, pos)

	if err != nil {
		return nil, err
	}

	return &mapIterator{
		sequenceIter: cur,
	}, nil
}

func (m Map) BufferedIterator(ctx context.Context) (MapIterator, error) {
	return m.BufferedIteratorAt(ctx, 0)
}

func (m Map) BufferedIteratorAt(ctx context.Context, pos uint64) (MapIterator, error) {
	bufCur, err := newBufferedIteratorAtIndex(ctx, m.orderedSequence, pos)

	if err != nil {
		return nil, err
	}

	return &mapIterator{
		sequenceIter: bufCur,
	}, nil
}

func (m Map) IteratorFrom(ctx context.Context, key Value) (MapIterator, error) {
	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)

	if err != nil {
		return nil, err
	}

	return &mapIterator{sequenceIter: cur}, nil
}

func (m Map) IteratorBackFrom(ctx context.Context, key Value) (MapIterator, error) {
	cur, err := newCursorBackFromValue(ctx, m.orderedSequence, key)

	if err != nil {
		return nil, err
	}

	return &mapIterator{sequenceIter: cur}, nil
}

type mapIterAllCallback func(key, value Value) error

func (m Map) IterAll(ctx context.Context, cb mapIterAllCallback) error {
	var k Value
	err := iterAll(ctx, m, func(v Value, _ uint64) error {
		if k != nil {
			err := cb(k, v)

			if err != nil {
				return err
			}

			k = nil
		} else {
			k = v
		}

		return nil
	})

	if err != nil {
		return err
	}

	d.PanicIfFalse(k == nil)
	return nil
}

// IterFromCount iterates over count entries in the map starting from startKey. If startKey is empty, iteration starts from the beginning.
func (m Map) IterFromCount(ctx context.Context, startKey string, count uint64, cb func(id string, addr hash.Hash) error) error {
	if count == 0 {
		return nil
	}

	var startVal Value
	if startKey != "" {
		startVal = String(startKey)
	}

	cur, err := newCursorAtValue(ctx, m.orderedSequence, startVal, false, false)
	if err != nil {
		return err
	}

	var processed uint64
	for ; cur.valid() && processed < count; processed++ {
		item, err := cur.current()
		if err != nil {
			return err
		}

		entry := item.(mapEntry)
		key := string(entry.key.(String))
		val := entry.value.(Ref)

		if err := cb(key, val.TargetHash()); err != nil {
			return err
		}

		if _, err := cur.advance(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m Map) IterRange(ctx context.Context, startIdx, endIdx uint64, cb mapIterAllCallback) error {
	var k Value
	_, err := iterRange(ctx, m, startIdx, endIdx, func(v Value) error {
		if k != nil {
			err := cb(k, v)

			if err != nil {
				return err
			}

			k = nil
		} else {
			k = v
		}

		return nil
	})

	if err != nil {
		return err
	}

	d.PanicIfFalse(k == nil)
	return nil
}

func (m Map) IterFrom(ctx context.Context, start Value, cb mapIterCallback) error {
	cur, err := newCursorAtValue(ctx, m.orderedSequence, start, false, false)

	if err != nil {
		return err
	}

	return cur.iter(ctx, func(v interface{}) (bool, error) {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

func (m Map) Edit() *MapEditor {
	return NewMapEditor(m)
}

func buildMapData(ctx context.Context, vr ValueReader, values []Value) (mapEntrySlice, error) {
	if len(values) == 0 {
		return mapEntrySlice{}, nil
	}

	if len(values)%2 != 0 {
		d.Panic("Must specify even number of key/value pairs")
	}
	kvs := mapEntrySlice{
		make([]mapEntry, len(values)/2),
	}

	for i := 0; i < len(values); i += 2 {
		d.PanicIfTrue(values[i] == nil)
		d.PanicIfTrue(values[i+1] == nil)
		entry := mapEntry{values[i], values[i+1]}
		kvs.entries[i/2] = entry
	}

	uniqueSorted := mapEntrySlice{
		make([]mapEntry, 0, len(kvs.entries)),
	}

	err := SortWithErroringLess(ctx, vr.Format(), kvs)

	if err != nil {
		return mapEntrySlice{}, err
	}

	last := kvs.entries[0]
	for i := 1; i < kvs.Len(); i++ {
		kv := kvs.entries[i]
		if !kv.key.Equals(last.key) {
			uniqueSorted.entries = append(uniqueSorted.entries, last)
		}

		last = kv
	}

	return mapEntrySlice{
		append(uniqueSorted.entries, last),
	}, nil
}

func makeMapLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(ctx context.Context, level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		d.PanicIfFalse(level == 0)
		mapData := make([]mapEntry, len(items))

		var lastKey Value
		for i, v := range items {
			entry := v.(mapEntry)

			if lastKey != nil {
				isLess, err := lastKey.Less(ctx, vrw.Format(), entry.key)

				if err != nil {
					return nil, orderedKey{}, 0, err
				}

				d.PanicIfFalse(isLess)
			}

			lastKey = entry.key
			mapData[i] = entry
		}

		seq, err := newMapLeafSequence(vrw, mapData...)

		if err != nil {
			return nil, orderedKey{}, 0, err
		}

		m := newMap(seq)
		var key orderedKey
		if len(mapData) > 0 {
			key, err = newOrderedKey(mapData[len(mapData)-1].key, vrw.Format())

			if err != nil {
				return nil, orderedKey{}, 0, err
			}
		}

		return m, key, uint64(len(items)), nil
	}
}

func newEmptyMapSequenceChunker(ctx context.Context, vrw ValueReadWriter) (*sequenceChunker, error) {
	makeChunk := makeMapLeafChunkFn(vrw)
	makeParentChunk := newOrderedMetaSequenceChunkFn(MapKind, vrw)
	return newEmptySequenceChunker(ctx, vrw, makeChunk, makeParentChunk, newMapChunker, mapHashValueBytes)
}

func (m Map) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (m Map) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (m Map) String() string {
	panic("unreachable")
}

func (m Map) HumanReadableString() string {
	panic("unreachable")
}

// VisitMapLevelOrder writes hashes of internal node chunks to a writer
// delimited with a newline character and returns the number of chunks written and the total number of
// bytes written or an error if encountered
func VisitMapLevelOrder(m Map, cb func(h hash.Hash) (int64, error)) (int64, int64, error) {
	chunkCount := int64(0)
	byteCount := int64(0)

	curLevel := []Map{m}
	for len(curLevel) > 0 {
		nextLevel := []Map{}
		for _, m := range curLevel {
			if metaSeq, ok := m.orderedSequence.(metaSequence); ok {
				ts, err := metaSeq.tuples()
				if err != nil {
					return 0, 0, err
				}
				for _, t := range ts {
					r, err := t.ref()
					if err != nil {
						return 0, 0, err
					}

					n, err := cb(r.TargetHash())
					if err != nil {
						return 0, 0, err
					}

					chunkCount++
					byteCount += n

					v, err := r.TargetValue(context.Background(), m.valueReadWriter())
					if err != nil {
						return 0, 0, err
					}

					nextLevel = append(nextLevel, v.(Map))
				}
			} else if _, ok := m.orderedSequence.(mapLeafSequence); ok {

			}
		}
		curLevel = nextLevel
	}

	return chunkCount, byteCount, nil
}

// VisitMapLevelOrderSized passes hashes of internal node chunks to a callback in level order,
// batching and flushing chunks to prevent large levels from consuming excessive memory. It returns
// the total number of chunks and bytes read, or an error.
func VisitMapLevelOrderSized(ms []Map, batchSize int, cb func(h hash.Hash) (int64, error)) (int64, int64, error) {
	if len(ms) == 0 {
		return 0, 0, nil
	}
	if batchSize < 0 {
		return 0, 0, errors.New("invalid batch size")
	}

	chunkCount := int64(0)
	byteCount := int64(0)

	chunkHashes := []hash.Hash{}
	chunkMaps := []Map{}

	flush := func() error {
		for _, h := range chunkHashes {
			n, err := cb(h)
			if err != nil {
				return err
			}
			byteCount += n
		}
		chunkCount += int64(len(chunkHashes))
		cc, bc, err := VisitMapLevelOrderSized(chunkMaps, batchSize, cb)
		if err != nil {
			return err
		}
		chunkCount += cc
		byteCount += bc
		chunkHashes = []hash.Hash{}
		chunkMaps = []Map{}
		return nil
	}

	for _, m := range ms {
		if metaSeq, ok := m.orderedSequence.(metaSequence); ok {
			ts, err := metaSeq.tuples()
			if err != nil {
				return 0, 0, err
			}
			for _, t := range ts {
				r, err := t.ref()
				if err != nil {
					return 0, 0, err
				}

				chunkHashes = append(chunkHashes, r.TargetHash())
				v, err := r.TargetValue(context.Background(), m.valueReadWriter())
				if err != nil {
					return 0, 0, err
				}
				if cm, ok := v.(Map); ok {
					chunkMaps = append(chunkMaps, cm)
				}
			}
		} else if _, ok := m.orderedSequence.(mapLeafSequence); ok {
		}
		if len(chunkHashes) >= batchSize {
			if err := flush(); err != nil {
				return 0, 0, err
			}
		}
	}

	if err := flush(); err != nil {
		return 0, 0, err
	}

	return chunkCount, byteCount, nil
}

func IsMapLeaf(m Map) bool {
	return m.isLeaf()
}

func (m Map) IndexForKey(ctx context.Context, key Value) (int64, error) {
	orderedKey, err := newOrderedKey(key, m.Format())
	if err != nil {
		return 0, err
	}

	if metaSeq, ok := m.orderedSequence.(metaSequence); ok {
		return indexForKeyWithinSubtree(ctx, orderedKey, metaSeq, m.valueReadWriter())
	} else if leaf, ok := m.orderedSequence.(mapLeafSequence); ok {
		leafIdx, err := leaf.search(ctx, orderedKey)
		if err != nil {
			return 0, err
		}

		return int64(leafIdx), nil
	} else {
		return 0, errors.New("unknown sequence type")
	}
}

func indexForKeyWithinSubtree(ctx context.Context, key orderedKey, metaSeq metaSequence, vrw ValueReadWriter) (int64, error) {
	ts, err := metaSeq.tuples()
	if err != nil {
		return 0, err
	}

	var idx int64
	for _, t := range ts {
		tupleKey, err := t.key(vrw)
		if err != nil {
			return 0, err
		}

		isLess, err := key.Less(ctx, vrw.Format(), tupleKey)
		if err != nil {
			return 0, err
		}
		if !isLess {
			eq := tupleKey.v.Equals(key.v)
			if eq {
				return idx + int64(t.numLeaves()-1), nil
			} else {
				idx += int64(t.numLeaves())
			}
		} else {
			child, err := t.getChildSequence(ctx, vrw)
			if err != nil {
				return 0, err
			}

			if childMetaSeq, ok := child.(metaSequence); ok {
				subtreeIdx, err := indexForKeyWithinSubtree(ctx, key, childMetaSeq, vrw)
				if err != nil {
					return 0, err
				}
				return idx + subtreeIdx, nil
			} else if leaf, ok := child.(mapLeafSequence); ok {
				leafIdx, err := leaf.search(ctx, key)
				if err != nil {
					return 0, err
				}

				return idx + int64(leafIdx), nil
			} else {
				return 0, errors.New("unknown sequence type")
			}
		}
	}

	return idx, nil
}

// MapUnionConflictCB is a callback that is used to resolve a key collision.
// Callers should pass a callback that returns the resolved value.
type MapUnionConflictCB func(key Value, aValue Value, bValue Value) (Value, error)

// UnionMaps unions |a| and |b|. Colliding keys are returned to |cb|. As
// currently implemented, keys of |b| are inserted into |a|.
func UnionMaps(ctx context.Context, a Map, b Map, cb MapUnionConflictCB) (Map, error) {
	editor := NewMapEditor(a)

	aIter, err := a.Iterator(ctx)
	if err != nil {
		return EmptyMap, nil
	}
	bIter, err := b.Iterator(ctx)
	if err != nil {
		return EmptyMap, nil
	}

	aKey, aVal, err := aIter.Next(ctx)
	if err != nil {
		return EmptyMap, nil
	}
	bKey, bVal, err := bIter.Next(ctx)
	if err != nil {
		return EmptyMap, nil
	}

	for aKey != nil && bKey != nil {

		aLess, err := aKey.Less(ctx, a.format(), bKey)
		if err != nil {
			return EmptyMap, nil
		}

		if aLess {
			// take from a (which we already have)
			aKey, aVal, err = aIter.Next(ctx)
			if err != nil {
				return EmptyMap, nil
			}
			continue
		}

		if aKey.Equals(bKey) {
			// collision, delegate behavior to caller
			chosen, err := cb(aKey, aVal, bVal)
			if err != nil {
				return EmptyMap, nil
			}
			if !chosen.Equals(aVal) {
				editor.Set(aKey, chosen)
			}
			// advance a and b
			aKey, aVal, err = aIter.Next(ctx)
			if err != nil {
				return EmptyMap, nil
			}
			bKey, aVal, err = bIter.Next(ctx)
			if err != nil {
				return EmptyMap, nil
			}
			continue
		}

		// take from b
		editor.Set(bKey, bVal)
		bKey, bVal, err = bIter.Next(ctx)
		if err != nil {
			return EmptyMap, nil
		}
	}

	if aKey == nil && bKey == nil {
		return editor.Map(ctx)
	}

	if aKey == nil {
		// |a| is finished, take rest from |b|.
		for bKey != nil {
			editor.Set(bKey, bVal)
			bKey, bVal, err = bIter.Next(ctx)
			if err != nil {
				return EmptyMap, nil
			}
		}
	}

	return editor.Map(ctx)
}
