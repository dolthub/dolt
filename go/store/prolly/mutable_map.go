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

type MutableMap struct {
	m       Map
	overlay val.TupleMap
}

func newMutableMap(m Map) MutableMap {
	return MutableMap{
		m:       m,
		overlay: val.NewTupleMap(m.keyDesc),
	}
}

func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	return applyEdits(ctx, mut.m, mut.overlay.Iter())
}

func (mut MutableMap) Put(ctx context.Context, key, value val.Tuple) (err error) {
	ok := mut.overlay.Put(key, value)
	if !ok {
		// synchronously flush overlay
		mut.m, err = mut.Map(ctx)
		mut.overlay = val.NewTupleMap(mut.m.keyDesc)
	}
	return
}

func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	value, ok := mut.overlay.Get(key)
	if ok {
		return cb(key, value)
	}
	return mut.m.Get(ctx, key, cb)
}

func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	if ok = mut.overlay.Has(key); ok {
		return
	}
	return mut.m.Has(ctx, key)
}

type editProvider interface {
	Count() int
	Next() (key, val val.Tuple)
	Close() error
}

var _ editProvider = val.KeyValueIter{}

func applyEdits(ctx context.Context, m Map, edits editProvider) (Map, error) {
	var err error
	if edits.Count() == 0 {
		return m, err
	}

	key, value := edits.Next()

	cur, err := mapCursorAtKey(ctx, m, key)
	if err != nil {
		return m, err
	}

	ch, err := newTreeChunker(ctx, cur, 0, m.nrw, newDefaultNodeSplitter)
	if err != nil {
		return m, err
	}

	for key != nil {

		var oldValue val.Tuple
		if cur.valid() {
			k, v, err := getKeyValue(ctx, cur)
			if err != nil {
				return m, err
			}
			if compareValues(m, key, k) == 0 {
				oldValue = v
			}
		}

		if oldValue == nil && value == nil {
			continue // already non-present
		}
		if oldValue != nil && compareValues(m, value, oldValue) == 0 {
			continue // same value
		}

		err = ch.advanceTo(ctx, cur)
		if err != nil {
			return m, err
		}

		if oldValue != nil {
			// stats.Modifications++
			if err = ch.Skip(ctx); err != nil {
				return m, err
			}
		} // else stats.Additions++

		if value != nil {
			_, err = ch.Append(ctx, nodeItem(key), nodeItem(value))
			if err != nil {
				continue
			}
		}

		key, value = edits.Next()
	}

	m.root, err = ch.Done(ctx)
	if err != nil {
		return m, err
	}

	return m, nil
}

func mapCursorAtKey(ctx context.Context, m Map, key val.Tuple) (*nodeCursor, error) {
	cur, err := newCursorAtItem(ctx, m.nrw, m.root, nodeItem(key), m.searchNode)
	return &cur, err
}

func getKeyValue(ctx context.Context, cur *nodeCursor) (key, value val.Tuple, err error) {
	key = val.Tuple(cur.current())

	if _, err = cur.advance(ctx); err != nil {
		return nil, nil, err
	}

	value = val.Tuple(cur.current())
	return
}

func compareKeys(m Map, left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}

func compareValues(m Map, left, right val.Tuple) int {
	return int(m.valDesc.Compare(left, right))
}
