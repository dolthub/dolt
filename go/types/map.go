// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type Map struct {
	seq orderedSequence
	h   *hash.Hash
}

func newMap(seq orderedSequence) Map {
	return Map{seq, &hash.Hash{}}
}

func mapHashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	entry := item.(mapEntry)
	hashValueBytes(entry.key, rv)
	hashValueBytes(entry.value, rv)
}

func NewMap(kv ...Value) Map {
	entries := buildMapData(kv)
	ch := newEmptyMapSequenceChunker(nil, nil)

	for _, entry := range entries {
		ch.Append(entry)
	}

	return newMap(ch.Done().(orderedSequence))
}

func NewStreamingMap(vrw ValueReadWriter, kvs <-chan Value) <-chan Map {
	var k Value
	outChan := make(chan Map, 1)
	go func() {
		defer close(outChan)
		gb := NewGraphBuilder(vrw, MapKind, false)
		for v := range kvs {
			if k == nil {
				k = v
				continue
			}
			gb.MapSet(nil, k, v)
			k = nil
		}
		d.PanicIfFalse(k == nil)
		outChan <- gb.Build().(Map)
	}()
	return outChan
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (m Map) Diff(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffTopDown(last.seq, m.seq, changes, closeChan)
}

// DiffHybrid computes the diff from |last| to |m| using a hybrid algorithm
// which balances returning results early vs completing quickly, if possible.
func (m Map) DiffHybrid(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffBest(last.seq, m.seq, changes, closeChan)
}

// DiffLeftRight computes the diff from |last| to |m| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (m Map) DiffLeftRight(last Map, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if m.Equals(last) {
		return
	}
	orderedSequenceDiffLeftRight(last.seq, m.seq, changes, closeChan)
}

// Collection interface
func (m Map) Len() uint64 {
	return m.seq.numLeaves()
}

func (m Map) Empty() bool {
	return m.Len() == 0
}

func (m Map) sequence() sequence {
	return m.seq
}

func (m Map) hashPointer() *hash.Hash {
	return m.h
}

// Value interface
func (m Map) Equals(other Value) bool {
	return m.Hash() == other.Hash()
}

func (m Map) Less(other Value) bool {
	return valueLess(m, other)
}

func (m Map) Hash() hash.Hash {
	if m.h.IsEmpty() {
		*m.h = getHash(m)
	}

	return *m.h
}

func (m Map) WalkValues(cb ValueCallback) {
	m.IterAll(func(k, v Value) {
		cb(k)
		cb(v)
	})
	return
}

func (m Map) WalkRefs(cb RefCallback) {
	m.seq.WalkRefs(cb)
}

func (m Map) typeOf() *Type {
	return m.seq.typeOf()
}

func (m Map) Kind() NomsKind {
	return MapKind
}

func (m Map) firstOrLast(last bool) (Value, Value) {
	cur := newCursorAt(m.seq, emptyKey, false, last, false)
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

	cur := newCursorAtIndex(m.seq, idx, false)
	entry := cur.current().(mapEntry)
	return entry.key, entry.value
}

func (m Map) MaybeGet(key Value) (v Value, ok bool) {
	cur := newCursorAtValue(m.seq, key, false, false, false)
	if !cur.valid() {
		return nil, false
	}
	entry := cur.current().(mapEntry)
	if !entry.key.Equals(key) {
		return nil, false
	}

	return entry.value, true
}

func (m Map) Set(key Value, val Value) Map {
	return m.SetM(key, val)
}

func (m Map) SetM(kv ...Value) Map {
	if len(kv) == 0 {
		return m
	}
	d.PanicIfFalse(len(kv)%2 == 0)

	k, v, tail := kv[0], kv[1], kv[2:]

	cur, found := m.getCursorAtValue(k, false)
	deleteCount := uint64(0)
	if found {
		deleteCount = 1
	}
	return m.splice(cur, deleteCount, mapEntry{k, v}).SetM(tail...)
}

func (m Map) Remove(k Value) Map {
	if cur, found := m.getCursorAtValue(k, false); found {
		return m.splice(cur, 1)
	}
	return m
}

func (m Map) splice(cur *sequenceCursor, deleteCount uint64, vs ...mapEntry) Map {
	ch := newSequenceChunker(cur, m.seq.valueReader(), nil, makeMapLeafChunkFn(m.seq.valueReader()), newOrderedMetaSequenceChunkFn(MapKind, m.seq.valueReader()), mapHashValueBytes)
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range vs {
		ch.Append(v)
	}
	return newMap(ch.Done().(orderedSequence))
}

func (m Map) getCursorAtValue(v Value, readAhead bool) (cur *sequenceCursor, found bool) {
	cur = newCursorAtValue(m.seq, v, true, false, readAhead)
	found = cur.idx < cur.seq.seqLen() && cur.current().(mapEntry).key.Equals(v)
	return
}

func (m Map) Has(key Value) bool {
	cur := newCursorAtValue(m.seq, key, false, false, false)
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
	cur := newCursorAt(m.seq, emptyKey, false, false, false)
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
		cursor: newCursorAtIndex(m.seq, pos, false),
	}
}

func (m Map) IteratorFrom(key Value) MapIterator {
	return &mapIterator{
		cursor: newCursorAtValue(m.seq, key, false, false, false),
	}
}

type mapIterAllCallback func(key, value Value)

func (m Map) IterAll(cb mapIterAllCallback) {
	cur := newCursorAt(m.seq, emptyKey, false, false, true)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		cb(entry.key, entry.value)
		return false
	})
}

func (m Map) IterFrom(start Value, cb mapIterCallback) {
	cur := newCursorAtValue(m.seq, start, false, false, false)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

func buildMapData(values []Value) mapEntrySlice {
	if len(values) == 0 {
		return mapEntrySlice{}
	}

	// Sadly, d.Chk.Equals() costs too much. BUG #83
	if 0 != len(values)%2 {
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

// If |vw| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func makeMapLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		mapData := make([]mapEntry, len(items), len(items))

		for i, v := range items {
			mapData[i] = v.(mapEntry)
		}

		m := newMap(newMapLeafSequence(vr, mapData...))
		var key orderedKey
		if len(mapData) > 0 {
			key = newOrderedKey(mapData[len(mapData)-1].key)
		}
		return m, key, uint64(len(items))
	}
}

func newEmptyMapSequenceChunker(vr ValueReader, vw ValueWriter) *sequenceChunker {
	return newEmptySequenceChunker(vr, vw, makeMapLeafChunkFn(vr), newOrderedMetaSequenceChunkFn(MapKind, vr), mapHashValueBytes)
}
