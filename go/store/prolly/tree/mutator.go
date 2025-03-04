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
	"fmt"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

type MutationIter interface {
	NextMutation(ctx context.Context) (key, value Item)
	Close() error
}

// ApplyMutations applies a sorted series of edits to a NodeStore,
// returning the new root Node.
//
// The algorithm is structured as follows:
//
//   - Create a new chunker, the main interface for building a new
//     tree.
//
//   - Create two cursors into the previous tree. Both cursors
//     track key indexes in the old keyspace. The first tracks where
//     a new edit will be applied relative to the old keyspace.
//     The second indicates the most recent edit in the new tree
//     relative to the old keyspace. The second cursor is embedded in
//     the chunker, maintained by the chunker, and necessary precedes
//     the first.
//
//   - For every edit, first identify the key index in the old keyspace
//     where the edit will be applied, and move the tracking cursor to
//     that index.
//
//   - Advance the chunker and the second cursor to the new edit point.
//     Refer to the chunker.AdvanceTo docstring for details.
//
//   - Add the edit to the chunker. This applies the edit to the in-progress
//     NodeStore. The new NodeStore may expand or shrink relative to the
//     old tree, but these details are internal to the chunker.
//
//   - Repeat for every edit.
//
//   - Finalize the chunker and resolve the tree's new root Node.
func ApplyMutations[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	root Node,
	order O,
	serializer S,
	edits MutationIter,
) (Node, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		return root, nil // no mutations
	}

	cur, err := newCursorAtKey(ctx, ns, root, K(newKey), order)
	if err != nil {
		return Node{}, err
	}

	chkr, err := newChunker(ctx, cur.clone(), 0, ns, serializer)
	if err != nil {
		return Node{}, err
	}

	for newKey != nil {

		// move |cur| to the NextMutation mutation point
		err = Seek(ctx, cur, K(newKey), order)
		if err != nil {
			return Node{}, err
		}

		var oldValue Item
		if cur.Valid() {
			// Compare mutations |newKey| and |newValue|
			// to the existing pair from the cursor
			if order.Compare(ctx, K(newKey), K(cur.CurrentKey())) == 0 {
				oldValue = cur.currentValue()
			}

			// check for no-op mutations
			// this includes comparing the key bytes because two equal keys may have different bytes,
			// in which case we need to update the index to match the bytes in the table.
			if equalValues(newValue, oldValue) && bytes.Equal(newKey, cur.CurrentKey()) {
				newKey, newValue = edits.NextMutation(ctx)
				continue
			}
		}

		if oldValue == nil && newValue == nil {
			// Don't try to delete what isn't there.
			newKey, newValue = edits.NextMutation(ctx)
			continue
		}

		// move |chkr| to the NextMutation mutation point
		err = chkr.advanceTo(ctx, cur)
		if err != nil {
			return Node{}, err
		}

		if oldValue == nil {
			err = chkr.AddPair(ctx, newKey, newValue)
		} else {
			if newValue != nil {
				err = chkr.UpdatePair(ctx, newKey, newValue)
			} else {
				err = chkr.DeletePair(ctx, newKey, oldValue)
			}
		}
		if err != nil {
			return Node{}, err
		}

		prev := newKey
		newKey, newValue = edits.NextMutation(ctx)
		if newKey != nil {
			assertTrue(order.Compare(ctx, K(newKey), K(prev)) > 0, "expected sorted edits"+fmt.Sprintf("%v, %v", prev, newKey))
		}
	}

	return chkr.Done(ctx)
}

func equalValues(left, right Item) bool {
	return bytes.Equal(left, right)
}
