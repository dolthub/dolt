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

type mutationIter interface {
	next() (key, val val.Tuple)
	count() int
	close() error
}

var _ mutationIter = memIter{}

func materializeMutations(ctx context.Context, m Map, edits mutationIter) (Map, error) {
	var err error
	if edits.count() == 0 {
		return m, err
	}

	key, value := edits.next()

	cur, err := mapCursorAtKey(ctx, m, key)
	if err != nil {
		return m, err
	}

	ch, err := newTreeChunker(ctx, cur, 0, m.ns, newDefaultNodeSplitter)
	if err != nil {
		return m, err
	}

	for key != nil {

		var oldValue val.Tuple
		if cur.valid() {
			k, v := getKeyValue(ctx, cur)
			if compareKeys(m, key, k) == 0 {
				oldValue = v
			}
		}

		if oldValue == nil && value == nil {
			key, value = edits.next()
			continue // already non-present
		}
		if oldValue != nil && compareValues(m, value, oldValue) == 0 {
			key, value = edits.next()
			continue // same value
		}

		err = ch.advanceTo(ctx, cur)
		if err != nil {
			return m, err
		}

		if oldValue != nil {
			// delete or update
			if err = ch.Skip(ctx); err != nil {
				return m, err
			}
		} // else insert

		if value != nil {
			// update or insert
			_, err = ch.Append(ctx, nodeItem(key), nodeItem(value))
			if err != nil {
				return Map{}, err
			}
		}

		key, value = edits.next()
	}

	m.root, err = ch.Done(ctx)
	if err != nil {
		return m, err
	}

	return m, nil
}

func mapCursorAtKey(ctx context.Context, m Map, key val.Tuple) (*nodeCursor, error) {
	cur, err := newCursorAtItem(ctx, m.ns, m.root, nodeItem(key), m.searchNode)
	return &cur, err
}

func getKeyValue(ctx context.Context, cur *nodeCursor) (key, value val.Tuple) {
	p := cur.currentPair()
	key, value = val.Tuple(p.key()), val.Tuple(p.value())
	return
}

func compareKeys(m Map, left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}

func compareValues(m Map, left, right val.Tuple) int {
	return int(m.valDesc.Compare(left, right))
}
