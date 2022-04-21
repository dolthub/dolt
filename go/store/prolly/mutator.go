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
	nextMutation(ctx context.Context) (key, value val.Tuple)
	close() error
}

var _ mutationIter = &memRangeIter{}

func materializeMutations(ctx context.Context, m Map, edits mutationIter) (Map, error) {
	newKey, newValue := edits.nextMutation(ctx)
	if newKey == nil {
		return m, nil // no mutations
	}

	cur, err := NewCursorAtItem(ctx, m.ns, m.root, NodeItem(newKey), m.searchNode)
	if err != nil {
		return m, err
	}

	chunker, err := newTreeChunker(ctx, cur.Clone(), 0, m.ns, defaultSplitterFactory)
	if err != nil {
		return m, err
	}

	for newKey != nil {

		// move |cur| to the nextMutation mutation point
		err = cur.seek(ctx, NodeItem(newKey), m.compareItems)
		if err != nil {
			return Map{}, err
		}

		var oldValue val.Tuple
		if cur.Valid() {
			// Compare mutations |newKey| and |newValue|
			// to the existing pair from the cursor
			oldKey := val.Tuple(cur.CurrentKey())
			if compareKeys(m, newKey, oldKey) == 0 {
				oldValue = val.Tuple(cur.CurrentValue())
			}
		}

		// check for no-op mutations
		if oldValue == nil && newValue == nil {
			newKey, newValue = edits.nextMutation(ctx)
			continue // already non-present
		}
		if oldValue != nil && compareValues(m, newValue, oldValue) == 0 {
			newKey, newValue = edits.nextMutation(ctx)
			continue // same newValue
		}

		// move |chunker| to the nextMutation mutation point
		err = chunker.AdvanceTo(ctx, cur)
		if err != nil {
			return m, err
		}

		if oldValue == nil {
			err = chunker.AddPair(ctx, newKey, newValue)
		} else {
			if newValue != nil {
				err = chunker.UpdatePair(ctx, newKey, newValue)
			} else {
				err = chunker.DeletePair(ctx, newKey, oldValue)
			}
		}
		if err != nil {
			return m, err
		}

		newKey, newValue = edits.nextMutation(ctx)
	}

	m.root, err = chunker.Done(ctx)
	if err != nil {
		return m, err
	}

	return m, nil
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
