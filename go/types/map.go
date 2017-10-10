// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"sort"

	"github.com/attic-labs/noms/go/d"
)

type Map struct {
	orderedSequence
}

func newMap(seq orderedSequence) Map {
	return Map{seq}
}

func mapHashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	entry := item.(mapEntry)
	hashValueBytes(entry.key, rv)
	hashValueBytes(entry.value, rv)
}

func NewMap(vrw ValueReadWriter, kv ...Value) Map {
	entries := buildMapData(kv)
	ch := newEmptyMapSequenceChunker(vrw)

	for _, entry := range entries {
		ch.Append(entry)
	}

	return newMap(ch.Done().(orderedSequence))
}

// NewStreamingMap takes an input channel of values and returns a output
// channel that will produce a finished Map. Values sent to the input channel
// must be alternating keys and values. (e.g. k1, v1, k2, v2...). Moreover keys
// need to be added to the channel in Noms sortorder, adding key values to the
// input channel out of order will result in a panic. Once the input channel is
// closed by the caller, a finished Map will be sent to the output channel. See
// graph_builder.go for building collections with values that are not in order.
func NewStreamingMap(vrw ValueReadWriter, kvs <-chan Value) <-chan Map {
	d.PanicIfTrue(vrw == nil)
	return newStreamingMap(vrw, kvs, func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map) {
		go readMapInput(vrw, kvs, outChan)
	})
}

type streamingMapReadFunc func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map)

func newStreamingMap(vrw ValueReadWriter, kvs <-chan Value, readFunc streamingMapReadFunc) <-chan Map {
	outChan := make(chan Map, 1)
	readFunc(vrw, kvs, outChan)
	return outChan
}

func readMapInput(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map) {
	defer close(outChan)
	ch := newEmptyMapSequenceChunker(vrw)
	var lastK Value
	nextIsKey := true
	var k Value
	for v := range kvs {
		d.PanicIfTrue(v == nil)
		if nextIsKey {
			k = v
			d.PanicIfFalse(lastK == nil || lastK.Less(k))
			lastK = k
			nextIsKey = false
			continue
		}
		ch.Append(mapEntry{key: k, value: v})
		nextIsKey = true
	}
	outChan <- newMap(ch.Done().(orderedSequence))
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (m Map) Diff(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffTopDown(last.orderedSequence, m.orderedSequence, changes, closeChan)
}

// DiffHybrid computes the diff from |last| to |m| using a hybrid algorithm
// which balances returning results early vs completing quickly, if possible.
func (m Map) DiffHybrid(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffBest(last.orderedSequence, m.orderedSequence, changes, closeChan)
}

// DiffLeftRight computes the diff from |last| to |m| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (m Map) DiffLeftRight(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffLeftRight(last.orderedSequence, m.orderedSequence, changes, closeChan)
}

// Collection interface

func (m Map) asSequence() sequence {
	return m.orderedSequence
}

// Value interface
func (m Map) Value() Value {
	return m
}

func (m Map) WalkValues(cb ValueCallback) {
	iterAll(m, func(v Value, idx uint64) {
		cb(v)
	})
	return
}

func (m Map) firstOrLast(last bool) (Value, Value) {
	cur := newCursorAt(m.orderedSequence, emptyKey, false, last)
	if !cur.valid() {
		return nil, nil
	}
	entry := cur.current().(mapEntry)
	return entry.key, entry.value
}

func (m Map) First() (Value, Value) {
	return m.firstOrLast(false)
}

func (m Map) Last() (Value, Value) {
	return m.firstOrLast(true)
}

func (m Map) At(idx uint64) (key, value Value) {
	if idx >= m.Len() {
		panic(fmt.Errorf("Out of bounds: %d >= %d", idx, m.Len()))
	}

	cur := newCursorAtIndex(m.orderedSequence, idx)
	entry := cur.current().(mapEntry)
	return entry.key, entry.value
}

func (m Map) MaybeGet(key Value) (v Value, ok bool) {
	cur := newCursorAtValue(m.orderedSequence, key, false, false)
	if !cur.valid() {
		return nil, false
	}
	entry := cur.current().(mapEntry)
	if !entry.key.Equals(key) {
		return nil, false
	}

	return entry.value, true
}

func (m Map) Has(key Value) bool {
	cur := newCursorAtValue(m.orderedSequence, key, false, false)
	if !cur.valid() {
		return false
	}
	entry := cur.current().(mapEntry)
	return entry.key.Equals(key)
}

func (m Map) Get(key Value) Value {
	v, _ := m.MaybeGet(key)
	return v
}

type mapIterCallback func(key, value Value) (stop bool)

func (m Map) Iter(cb mapIterCallback) {
	cur := newCursorAt(m.orderedSequence, emptyKey, false, false)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

// Any returns true if cb() return true for any of the items in the map.
func (m Map) Any(cb func(k, v Value) bool) (yep bool) {
	m.Iter(func(k, v Value) bool {
		if cb(k, v) {
			yep = true
			return true
		}
		return false
	})
	return
}

func (m Map) Iterator() MapIterator {
	return m.IteratorAt(0)
}

func (m Map) IteratorAt(pos uint64) MapIterator {
	return &mapIterator{
		cursor: newCursorAtIndex(m.orderedSequence, pos),
	}
}

func (m Map) IteratorFrom(key Value) MapIterator {
	return &mapIterator{
		cursor: newCursorAtValue(m.orderedSequence, key, false, false),
	}
}

type mapIterAllCallback func(key, value Value)

func (m Map) IterAll(cb mapIterAllCallback) {
	var k Value
	iterAll(m, func(v Value, idx uint64) {
		if k != nil {
			cb(k, v)
			k = nil
		} else {
			k = v
		}
	})
	d.PanicIfFalse(k == nil)
}

func (m Map) IterFrom(start Value, cb mapIterCallback) {
	cur := newCursorAtValue(m.orderedSequence, start, false, false)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

func (m Map) Edit() *MapEditor {
	return NewMapEditor(m)
}

func buildMapData(values []Value) mapEntrySlice {
	if len(values) == 0 {
		return mapEntrySlice{}
	}

	if len(values)%2 != 0 {
		d.Panic("Must specify even number of key/value pairs")
	}
	kvs := make(mapEntrySlice, len(values)/2)

	for i := 0; i < len(values); i += 2 {
		d.PanicIfTrue(values[i] == nil)
		d.PanicIfTrue(values[i+1] == nil)
		entry := mapEntry{values[i], values[i+1]}
		kvs[i/2] = entry
	}

	uniqueSorted := make(mapEntrySlice, 0, len(kvs))
	sort.Stable(kvs)
	last := kvs[0]
	for i := 1; i < len(kvs); i++ {
		kv := kvs[i]
		if !kv.key.Equals(last.key) {
			uniqueSorted = append(uniqueSorted, last)
		}

		last = kv
	}

	return append(uniqueSorted, last)
}

func makeMapLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		d.PanicIfFalse(level == 0)
		mapData := make([]mapEntry, len(items), len(items))

		var lastKey Value
		for i, v := range items {
			entry := v.(mapEntry)
			d.PanicIfFalse(lastKey == nil || lastKey.Less(entry.key))
			lastKey = entry.key
			mapData[i] = entry
		}

		m := newMap(newMapLeafSequence(vrw, mapData...))
		var key orderedKey
		if len(mapData) > 0 {
			key = newOrderedKey(mapData[len(mapData)-1].key)
		}
		return m, key, uint64(len(items))
	}
}

func newEmptyMapSequenceChunker(vrw ValueReadWriter) *sequenceChunker {
	return newEmptySequenceChunker(vrw, makeMapLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(MapKind, vrw), mapHashValueBytes)
}
