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

	newKey, newValue := edits.next()

	cur, err := mapCursorAtKey(ctx, m, newKey)
	if err != nil {
		return m, err
	}

	chunker, err := newTreeChunker(ctx, cur.clone(), 0, m.ns, newDefaultNodeSplitter)
	if err != nil {
		return m, err
	}

	for newKey != nil {

		// move |cur| to the next mutation point
		err = cur.seek(ctx, nodeItem(newKey), m.compareItems)
		if err != nil {
			return Map{}, err
		}

		var oldValue val.Tuple
		if cur.valid() {
			// compare mutations |newKey| and |newValue|
			// to the existing pair from the cursor
			k, v := getKeyValuePair(ctx, cur)
			if compareKeys(m, newKey, k) == 0 {
				oldValue = v
			}
		}

		if oldValue == nil && newValue == nil {
			newKey, newValue = edits.next()
			continue // already non-present
		}
		if oldValue != nil && compareValues(m, newValue, oldValue) == 0 {
			newKey, newValue = edits.next()
			continue // same newValue
		}

		// move |chunker| to the next mutation point
		err = chunker.advanceTo(ctx, cur)
		if err != nil {
			return m, err
		}

		if oldValue != nil {
			// delete or update
			if err = chunker.Skip(ctx); err != nil {
				return m, err
			}
		} // else insert

		if newValue != nil {
			// update or insert
			_, err = chunker.Append(ctx, nodeItem(newKey), nodeItem(newValue))
			if err != nil {
				return Map{}, err
			}
		}

		newKey, newValue = edits.next()
	}

	m.root, err = chunker.Done(ctx)
	if err != nil {
		return m, err
	}

	return m, nil
}

func mapCursorAtKey(ctx context.Context, m Map, key val.Tuple) (*nodeCursor, error) {
	cur, err := newCursorAtItem(ctx, m.ns, m.root, nodeItem(key), m.searchNode)
	return &cur, err
}

func getKeyValuePair(ctx context.Context, cur *nodeCursor) (key, value val.Tuple) {
	p := cur.currentPair()
	key, value = val.Tuple(p.key()), val.Tuple(p.value())
	return
}

func compareKeys(m Map, left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}

func compareValues(m Map, left, right val.Tuple) int {
	// order NULLs last
	if left == nil {
		if right == nil {
			return 0
		} else {
			return 1
		}
	} else if right == nil {
		if left == nil {
			return 0
		} else {
			return -1
		}
	}
	return m.valDesc.Compare(left, right)
}
