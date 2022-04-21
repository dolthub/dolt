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

package tree

import (
	"bytes"
	"context"

	"github.com/dolthub/dolt/go/store/val"
)

type MutationIter interface {
	NextMutation(ctx context.Context) (key, value NodeItem)
	Close() error
}

func ApplyMutations(
	ctx context.Context,
	ns NodeStore,
	root Node,
	edits MutationIter,
	search ItemSearchFn,
	compare CompareFn,
) (Node, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		return root, nil // no mutations
	}

	cur, err := NewCursorAtItem(ctx, ns, root, newKey, search)
	if err != nil {
		return Node{}, err
	}

	chunker, err := newTreeChunker(ctx, cur.Clone(), 0, ns, defaultSplitterFactory)
	if err != nil {
		return Node{}, err
	}

	for newKey != nil {

		// move |cur| to the NextMutation mutation point
		err = cur.seek(ctx, newKey, compare)
		if err != nil {
			return Node{}, err
		}

		var oldValue NodeItem
		if cur.Valid() {
			// Compare mutations |newKey| and |newValue|
			// to the existing pair from the cursor
			if compare(newKey, cur.CurrentKey()) == 0 {
				oldValue = cur.CurrentValue()
			}
		}

		// check for no-op mutations
		if oldValue == nil && newValue == nil {
			newKey, newValue = edits.NextMutation(ctx)
			continue // already non-present
		}
		if oldValue != nil && equalValues(newValue, oldValue) {
			newKey, newValue = edits.NextMutation(ctx)
			continue // same newValue
		}

		// move |chunker| to the NextMutation mutation point
		err = chunker.AdvanceTo(ctx, cur)
		if err != nil {
			return Node{}, err
		}

		if oldValue == nil {
			err = chunker.AddPair(ctx, val.Tuple(newKey), val.Tuple(newValue))
		} else {
			if newValue != nil {
				err = chunker.UpdatePair(ctx, val.Tuple(newKey), val.Tuple(newValue))
			} else {
				err = chunker.DeletePair(ctx, val.Tuple(newKey), val.Tuple(oldValue))
			}
		}
		if err != nil {
			return Node{}, err
		}

		newKey, newValue = edits.NextMutation(ctx)
	}

	return chunker.Done(ctx)
}

func equalValues(left, right NodeItem) bool {
	return bytes.Equal(left, right)
}
