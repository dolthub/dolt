// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/val"
)

type Map struct {
	root    node
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	// todo(andy): do we need a metaTuple descriptor?
	nrw NodeReadWriter
}

type KeyValueFn func(key, value val.Tuple) error

func (m Map) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	query := nodeItem(key)

	err = newCursorAtItem(ctx, m.nrw, m.root, query, m.compareKeys, func(cur *nodeCursor) error {

		var k, v val.Tuple
		if m.compareKeys(query, cur.current()) == 0 {
			k = val.Tuple(cur.current())

			if _, err = cur.advance(ctx); err != nil {
				return err
			}

			v = val.Tuple(cur.current())
		}

		return cb(k, v)
	})

	return err
}

func (m Map) compareKeys(left, right nodeItem) int {
	l, r := val.Tuple(left), val.Tuple(right)
	return int(m.keyDesc.Compare(l, r))
}

//
//func NewMap(ctx context.Context, vrw NodeReadWriter, items ...nodeItem) (Map, error) {
//	if len(items)%2 == 1 {
//		panic("items must be even length")
//	}
//
//	ch, err := newEmptyMapSequenceChunker(ctx, vrw)
//	if err != nil {
//		return EmptyMap, err
//	}
//
//	for i := 0; i < len(items); i += 2 {
//		_, err := ch.Append(ctx, items[i])
//		if err != nil {
//			return EmptyMap, err
//		}
//	}
//
//	seq, err := ch.Done(ctx)
//
//	if err != nil {
//		return EmptyMap, err
//	}
//
//	return newMap(seq.(orderedSequence)), nil
//}
//
//// Collection interface
//
//func (m Map) asSequence() node {
//	return m.orderedSequence
//}
//
//// Value interface
//func (m Map) Value(ctx context.Context) (Value, error) {
//	return m, nil
//}
//
//func (m Map) WalkValues(ctx context.Context, cb ValueCallback) error {
//	err := iterAll(ctx, m, func(v Value, idx uint64) error {
//		return cb(v)
//	})
//
//	return err
//}
//
//func (m Map) firstOrLast(ctx context.Context, last bool) (Value, Value, error) {
//	cur, err := newCursorAt(ctx, m.orderedSequence, emptyKey, false, last)
//
//	if err != nil {
//		return nil, nil, err
//	}
//
//	if !cur.valid() {
//		return nil, nil, nil
//	}
//
//	currItem, err := cur.current()
//
//	if err != nil {
//		return nil, nil, err
//	}
//
//	entry := currItem.(mapEntry)
//	return entry.key, entry.value, nil
//}
//
//func (m Map) Empty() bool {
//	if m.orderedSequence == nil {
//		return true
//	}
//
//	return m.orderedSequence.Empty()
//}
//
//func (m Map) First(ctx context.Context) (Value, Value, error) {
//	return m.firstOrLast(ctx, false)
//}
//
//func (m Map) Last(ctx context.Context) (Value, Value, error) {
//	return m.firstOrLast(ctx, true)
//}
//
//func (m Map) At(ctx context.Context, idx uint64) (key, value Value, err error) {
//	if idx >= m.Len() {
//		panic(fmt.Errorf("out of bounds: %d >= %d", idx, m.Len()))
//	}
//
//	cur, err := newSequenceIteratorAtIndex(ctx, m.orderedSequence, idx)
//
//	if err != nil {
//		return nil, nil, err
//	}
//
//	item, err := cur.current()
//
//	if err != nil {
//		return nil, nil, err
//	}
//
//	entry := item.(mapEntry)
//	return entry.key, entry.value, nil
//}
//
//func (m Map) MaybeGet(ctx context.Context, key Value) (v Value, ok bool, err error) {
//	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)
//
//	if err != nil {
//		return nil, false, err
//	}
//
//	if !cur.valid() {
//		return nil, false, nil
//	}
//
//	item, err := cur.current()
//
//	if err != nil {
//		return nil, false, err
//	}
//
//	entry := item.(mapEntry)
//
//	if !entry.key.Equals(key) {
//		return nil, false, nil
//	}
//
//	return entry.value, true, nil
//}
//
//func (m Map) MaybeGetTuple(ctx context.Context, key Tuple) (v Tuple, ok bool, err error) {
//	var val Value
//	val, ok, err = m.MaybeGet(ctx, key)
//
//	if val != nil {
//		return val.(Tuple), ok, err
//	}
//
//	return Tuple{}, ok, err
//}
//
//func (m Map) Has(ctx context.Context, key Value) (bool, error) {
//	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)
//
//	if err != nil {
//		return false, err
//	}
//
//	if !cur.valid() {
//		return false, nil
//	}
//
//	item, err := cur.current()
//
//	if err != nil {
//		return false, err
//	}
//
//	entry := item.(mapEntry)
//	return entry.key.Equals(key), nil
//}
//
//type mapIterCallback func(key, value Value) (stop bool, err error)
//
//func (m Map) Iter(ctx context.Context, cb mapIterCallback) error {
//	cur, err := newCursorAt(ctx, m.orderedSequence, emptyKey, false, false)
//
//	if err != nil {
//		return err
//	}
//
//	return cur.iter(ctx, func(v interface{}) (bool, error) {
//		entry := v.(mapEntry)
//		return cb(entry.key, entry.value)
//	})
//}
//
//// Any returns true if cb() return true for any of the items in the map.
//func (m Map) Any(ctx context.Context, cb func(k, v Value) bool) (yep bool, err error) {
//	err = m.Iter(ctx, func(k, v Value) (bool, error) {
//		if cb(k, v) {
//			yep = true
//			return true, nil
//		}
//		return false, nil
//	})
//
//	return yep, err
//}
//
//func (m Map) isPrimitive() bool {
//	return false
//}
//
//func (m Map) Iterator(ctx context.Context) (MapIterator, error) {
//	return m.IteratorAt(ctx, 0)
//}
//
//func (m Map) IteratorAt(ctx context.Context, pos uint64) (MapIterator, error) {
//	cur, err := newSequenceIteratorAtIndex(ctx, m.orderedSequence, pos)
//
//	if err != nil {
//		return nil, err
//	}
//
//	return &mapIterator{
//		sequenceIter: cur,
//	}, nil
//}
//
//func (m Map) BufferedIterator(ctx context.Context) (MapIterator, error) {
//	return m.BufferedIteratorAt(ctx, 0)
//}
//
//func (m Map) BufferedIteratorAt(ctx context.Context, pos uint64) (MapIterator, error) {
//	bufCur, err := newBufferedIteratorAtIndex(ctx, m.orderedSequence, pos)
//
//	if err != nil {
//		return nil, err
//	}
//
//	return &mapIterator{
//		sequenceIter: bufCur,
//	}, nil
//}
//
//func (m Map) IteratorFrom(ctx context.Context, key Value) (MapIterator, error) {
//	cur, err := newCursorAtValue(ctx, m.orderedSequence, key, false, false)
//
//	if err != nil {
//		return nil, err
//	}
//
//	return &mapIterator{sequenceIter: cur}, nil
//}
//
//func (m Map) IteratorBackFrom(ctx context.Context, key Value) (MapIterator, error) {
//	cur, err := newCursorBackFromValue(ctx, m.orderedSequence, key)
//
//	if err != nil {
//		return nil, err
//	}
//
//	return &mapIterator{sequenceIter: cur}, nil
//}
//
//type mapIterAllCallback func(key, value Value) error
//
//func (m Map) IterAll(ctx context.Context, cb mapIterAllCallback) error {
//	var k Value
//	err := iterAll(ctx, m, func(v Value, _ uint64) error {
//		if k != nil {
//			err := cb(k, v)
//
//			if err != nil {
//				return err
//			}
//
//			k = nil
//		} else {
//			k = v
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		return err
//	}
//
//	d.PanicIfFalse(k == nil)
//	return nil
//}
//
//func (m Map) IterRange(ctx context.Context, startIdx, endIdx uint64, cb mapIterAllCallback) error {
//	var k Value
//	_, err := iterRange(ctx, m, startIdx, endIdx, func(v Value) error {
//		if k != nil {
//			err := cb(k, v)
//
//			if err != nil {
//				return err
//			}
//
//			k = nil
//		} else {
//			k = v
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		return err
//	}
//
//	d.PanicIfFalse(k == nil)
//	return nil
//}
//
//func (m Map) IterFrom(ctx context.Context, start val.Tuple, cb mapIterCallback) error {
//	cur, err := newCursorAtValue(ctx, m.orderedSequence, start, false, false)
//
//	if err != nil {
//		return err
//	}
//
//	return cur.iter(ctx, func(v interface{}) (bool, error) {
//		entry := v.(mapEntry)
//		return cb(entry.key, entry.value)
//	})
//}
//
//func makeMapLeafChunkFn(vrw NodeReadWriter) makeChunkFn {
//	return func(level uint64, items []nodeItem) (Collection, orderedKey, uint64, error) {
//		d.PanicIfFalse(level == 0)
//		mapData := make([]mapEntry, len(items))
//
//		var lastKey Value
//		for i, v := range items {
//			entry := v.(mapEntry)
//
//			if lastKey != nil {
//				isLess, err := lastKey.Less(vrw.Format(), entry.key)
//
//				if err != nil {
//					return nil, orderedKey{}, 0, err
//				}
//
//				d.PanicIfFalse(isLess)
//			}
//
//			lastKey = entry.key
//			mapData[i] = entry
//		}
//
//		seq, err := newMapLeafSequence(vrw, mapData...)
//
//		if err != nil {
//			return nil, orderedKey{}, 0, err
//		}
//
//		m := newMap(seq)
//		var key orderedKey
//		if len(mapData) > 0 {
//			key, err = newOrderedKey(mapData[len(mapData)-1].key, vrw.Format())
//
//			if err != nil {
//				return nil, orderedKey{}, 0, err
//			}
//		}
//
//		return m, key, uint64(len(items)), nil
//	}
//}
//
//func newEmptyMapSequenceChunker(ctx context.Context, vrw NodeReadWriter) (*treeChunker, error) {
//	makeChunk := makeMapLeafChunkFn(vrw)
//	makeParentChunk := newOrderedMetaSequenceChunkFn(MapKind, vrw)
//	return newEmptyTreeChunker(ctx, vrw, makeChunk, makeParentChunk, newMapChunker, mapHashValueBytes)
//}
