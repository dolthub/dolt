// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/d"
)

var ErrKeysNotOrdered = errors.New("streaming map keys not ordered")

var EmptyMap Map

type Map struct {
	orderedSequence
}

func newMap(seq orderedSequence) Map {
	return Map{seq}
}

func mapHashValueBytes(item sequenceItem, rv *rollingValueHasher) error {
	entry := item.(mapEntry)
	err := hashValueBytes(entry.key, rv)

	if err != nil {
		return err
	}

	err = hashValueBytes(entry.value, rv)

	if err != nil {
		return err
	}

	return nil
}

func NewMap(ctx context.Context, vrw ValueReadWriter, kv ...Value) (Map, error) {
	entries, err := buildMapData(vrw.Format(), kv)

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

// NewStreamingMap takes an input channel of values and returns a output
// channel that will produce a finished Map. Values sent to the input channel
// must be alternating keys and values. (e.g. k1, v1, k2, v2...). Moreover keys
// need to be added to the channel in Noms sortorder, adding key values to the
// input channel out of order will result in a panic. Once the input channel is
// closed by the caller, a finished Map will be sent to the output channel. See
// graph_builder.go for building collections with values that are not in order.
func NewStreamingMap(ctx context.Context, vrw ValueReadWriter, ae *atomicerr.AtomicError, kvs <-chan Value) <-chan Map {
	d.PanicIfTrue(vrw == nil)
	return newStreamingMap(vrw, kvs, func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map) {
		go func() {
			readMapInput(ctx, vrw, ae, kvs, outChan)
		}()
	})
}

type streamingMapReadFunc func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map)

func newStreamingMap(vrw ValueReadWriter, kvs <-chan Value, readFunc streamingMapReadFunc) <-chan Map {
	outChan := make(chan Map, 1)
	readFunc(vrw, kvs, outChan)
	return outChan
}

func readMapInput(ctx context.Context, vrw ValueReadWriter, ae *atomicerr.AtomicError, kvs <-chan Value, outChan chan<- Map) {
	defer close(outChan)

	ch, err := newEmptyMapSequenceChunker(ctx, vrw)

	if ae.SetIfError(err) {
		return
	}

	var lastK Value
	nextIsKey := true
	var k Value
	for v := range kvs {
		if nextIsKey {
			k = v

			if lastK != nil {
				isLess, err := lastK.Less(vrw.Format(), k)

				if ae.SetIfError(err) {
					return
				}

				if !isLess {
					ae.SetIfError(ErrKeysNotOrdered)
					return
				}
			}
			lastK = k
			nextIsKey = false
			continue
		}
		_, err := ch.Append(ctx, mapEntry{key: k, value: v})

		if ae.SetIfError(err) {
			return
		}

		nextIsKey = true
	}

	seq, err := ch.Done(ctx)

	if ae.SetIfError(err) {
		return
	}

	outChan <- newMap(seq.(orderedSequence))
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (m Map) Diff(ctx context.Context, last Map, ae *atomicerr.AtomicError, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffTopDown(ctx, last.orderedSequence, m.orderedSequence, ae, changes, closeChan)
}

// DiffHybrid computes the diff from |last| to |m| using a hybrid algorithm
// which balances returning results early vs completing quickly, if possible.
func (m Map) DiffHybrid(ctx context.Context, last Map, ae *atomicerr.AtomicError, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffBest(ctx, last.orderedSequence, m.orderedSequence, ae, changes, closeChan)
}

// DiffLeftRight computes the diff from |last| to |m| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (m Map) DiffLeftRight(ctx context.Context, last Map, ae *atomicerr.AtomicError, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffLeftRight(ctx, last.orderedSequence, m.orderedSequence, ae, changes, closeChan)
}

// Collection interface

func (m Map) asSequence() sequence {
	return m.orderedSequence
}

// Value interface
func (m Map) Value(ctx context.Context) (Value, error) {
	return m, nil
}

func (m Map) WalkValues(ctx context.Context, cb ValueCallback) error {
	err := iterAll(ctx, m, func(v Value, idx uint64) error {
		return cb(v)
	})

	return err
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
	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)

	if err != nil {
		return nil, err
	}

	// kinda hacky, but a lot less work than implementing newCursorFromValueAtEnd which would have to search back
	cur.reverse = true
	if !cur.valid() {
		cur.advance(ctx)
	}

	item, err := cur.current()

	if err != nil {
		return nil, err
	}

	entry := item.(mapEntry)
	isLess, err := entry.key.Less(m.Format(), key)

	if err != nil {
		return nil, err
	}

	if !isLess && !key.Equals(entry.key) {
		_, err := cur.advance(ctx)

		if err != nil {
			return nil, err
		}
	}

	return &mapIterator{sequenceIter: cur}, nil
}

type mapIterAllCallback func(key, value Value) error

func (m Map) IterAll(ctx context.Context, cb mapIterAllCallback) error {
	var k Value
	err := iterAll(ctx, m, func(v Value, idx uint64) error {
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

func buildMapData(nbf *NomsBinFormat, values []Value) (mapEntrySlice, error) {
	if len(values) == 0 {
		return mapEntrySlice{}, nil
	}

	if len(values)%2 != 0 {
		d.Panic("Must specify even number of key/value pairs")
	}
	kvs := mapEntrySlice{
		make([]mapEntry, len(values)/2),
		nbf,
	}

	for i := 0; i < len(values); i += 2 {
		d.PanicIfTrue(values[i] == nil)
		d.PanicIfTrue(values[i+1] == nil)
		entry := mapEntry{values[i], values[i+1]}
		kvs.entries[i/2] = entry
	}

	uniqueSorted := mapEntrySlice{
		make([]mapEntry, 0, len(kvs.entries)),
		nbf,
	}

	err := SortWithErroringLess(kvs)

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
		uniqueSorted.nbf,
	}, nil
}

func makeMapLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		d.PanicIfFalse(level == 0)
		mapData := make([]mapEntry, len(items))

		var lastKey Value
		for i, v := range items {
			entry := v.(mapEntry)

			if lastKey != nil {
				isLess, err := lastKey.Less(vrw.Format(), entry.key)

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
	return newEmptySequenceChunker(ctx, vrw, makeMapLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(MapKind, vrw), mapHashValueBytes)
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
