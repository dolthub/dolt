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
	"fmt"

	"github.com/dolthub/dolt/go/store/val"
)

type Map struct {
	root    Node
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	// todo(andy): do we need a metaTuple descriptor?
	nrw NodeReadWriter
}

type KeyValueFn func(key, value val.Tuple) error

func NewMap(node Node, nrw NodeReadWriter, keyDesc, valDesc val.TupleDesc) Map {
	return Map{
		root:    node,
		keyDesc: keyDesc,
		valDesc: valDesc,
		nrw:     nrw,
	}
}

func MakeNewMap(ctx context.Context, nrw NodeReadWriter, keyDesc, valDesc val.TupleDesc, tups ...val.Tuple) (Map, error) {
	m := NewMap(nil, nrw, keyDesc, valDesc)

	ch, err := newEmptyTreeChunker(ctx, nrw, newDefaultNodeSplitter)
	if err != nil {
		return Map{}, err
	}

	if len(tups)%2 != 0 {
		return Map{}, fmt.Errorf("tuples must be key-value pairs")
	}

	for i := 0; i < len(tups); i += 2 {
		_, err = ch.Append(ctx, nodeItem(tups[i]), nodeItem(tups[i+1]))
		if err != nil {
			return Map{}, err
		}
	}

	m.root, err = ch.Done(ctx)
	if err != nil {
		return Map{}, err
	}

	return m, nil
}

func (m Map) Count() uint64 {
	return m.root.cumulativeCount() / 2
}

func (m Map) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	cur, err := newLeafCursorAtItem(ctx, m.nrw, m.root, nodeItem(key), m.searchNode)
	if err != nil {
		return err
	}

	var k, v val.Tuple
	if m.compareKeys(key, val.Tuple(cur.current())) == 0 {
		k = val.Tuple(cur.current())
		if _, err = cur.advance(ctx); err != nil {
			return err
		}

		v = val.Tuple(cur.current())
	}

	return cb(k, v)
}

func (m Map) GetIndex(ctx context.Context, idx uint64, cb KeyValueFn) (err error) {
	if idx > m.Count() {
		return fmt.Errorf("index is out of bounds for map")
	}

	treeIndex := idx * 2
	cur, err := newCursorAtIndex(ctx, m.nrw, m.root, treeIndex)
	if err != nil {
		return err
	}

	k := val.Tuple(cur.current())
	if _, err = cur.advance(ctx); err != nil {
		return err
	}
	v := val.Tuple(cur.current())

	return cb(k, v)
}

func (m Map) GetMany(ctx context.Context, keys []val.Tuple, cb KeyValueFn) (err error) {
	panic("unimplemented")
}

func (m Map) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	query := nodeItem(key)

	cur, err := newLeafCursorAtItem(ctx, m.nrw, m.root, query, m.searchNode)
	if err != nil {
		return false, err
	}

	ok = m.compareItems(query, cur.current()) == 0
	return
}

type ValueRange struct {
	startKey, stopKey val.Tuple
	inclusiveStart    bool
	inclusiveStop     bool
	// nil means unbounded
}

func (m Map) IterValueRange(ctx context.Context, r ValueRange, cb KeyValueFn) (err error) {
	panic("unimplemented")
}

type IndexRange struct {
	start, stop uint64
	// -1 means unbounded
}

func (m Map) IterIndexRange(ctx context.Context, r IndexRange, cb KeyValueFn) (err error) {
	if r.start < 0 {
		r.start = 0
	}
	if r.stop < 0 {
		r.stop = m.Count()
	}

	retreat := false
	count := r.stop - r.start
	if count < 0 {
		retreat = true
		count = -count
	}

	treeIndex := r.start * 2
	cur, err := newCursorAtIndex(ctx, m.nrw, m.root, treeIndex)
	if err != nil {
		return err
	}

	for i := uint64(0); i < count; i++ {
		k := val.Tuple(cur.current())
		if retreat {
			_, err = cur.retreat(ctx)
		} else {
			_, err = cur.advance(ctx)
		}
		if err != nil {
			return err
		}

		v := val.Tuple(cur.current())
		if retreat {
			_, err = cur.retreat(ctx)
		} else {
			_, err = cur.advance(ctx)
		}
		if err != nil {
			return err
		}

		if err = cb(k, v); err != nil {
			return err
		}
	}
	return
}

func (m Map) IterAll(ctx context.Context, cb KeyValueFn) (err error) {
	r := IndexRange{start: 0, stop: m.Count()}
	return m.IterIndexRange(ctx, r, cb)
}

func (m Map) searchNode(query nodeItem, nd Node) int {
	var card int
	if nd.level() == 0 {
		// leaf nodes
		card = 2
	} else {
		// internal nodes
		card = 1
	}

	n := nd.nodeCount() / card
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := m.compareItems(query, nd.getItem(h*card)) <= 0
		// i ≤ h < j
		if !less {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and
	// f(j) (= f(i)) == true  =>  answer is i.
	return i * card
}

func (m Map) compareItems(left, right nodeItem) int {
	l, r := val.Tuple(left), val.Tuple(right)
	return m.compareKeys(l, r)
}

func (m Map) compareKeys(left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}
