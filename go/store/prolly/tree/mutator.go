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
// - Create a new chunker, the main interface for building a new
//   tree.
// - Create two cursors into the previous tree. Both cursors
//   track key indexes in the old keyspace. The first tracks where
//   a new edit will be applied relative to the old keyspace.
//   The second indicates the most recent edit in the new tree
//   relative to the old keyspace. The second cursor is embedded in
//   the chunker, maintained by the chunker, and necessary precedes
//   the first.
//
// - For every edit, first identify the key index in the old keyspace
//   where the edit will be applied, and move the tracking cursor to
//   that index.
// - Advance the chunker and the second cursor to the new edit point.
//   Refer to the chunker.AdvanceTo docstring for details.
// - Add the edit to the chunker. This applies the edit to the in-progress
//   NodeStore. The new NodeStore may expand or shrink relative to the
//   old tree, but these details are internal to the chunker.
//  - Repeat for every edit.
//
//  - Finalize the chunker and resolve the tree's new root Node.
func ApplyMutations[S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	root Node,
	serializer S,
	edits MutationIter,
	compare CompareFn,
) (Node, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		return root, nil // no mutations
	}

	cur, err := NewCursorFromCompareFn(ctx, ns, root, newKey, compare)
	if err != nil {
		return Node{}, err
	}

	chkr, err := newChunker(ctx, cur.Clone(), 0, ns, serializer)
	if err != nil {
		return Node{}, err
	}

	for newKey != nil {

		// move |cur| to the NextMutation mutation point
		err = cur.seek(ctx, newKey, compare)
		if err != nil {
			return Node{}, err
		}

		var oldValue Item
		if cur.Valid() {
			// Compare mutations |newKey| and |newValue|
			// to the existing pair from the cursor
			if compare(newKey, cur.CurrentKey()) == 0 {
				oldValue = cur.CurrentValue()
			}
		}

		// check for no-op mutations
		if equalValues(newValue, oldValue) {
			newKey, newValue = edits.NextMutation(ctx)
			continue // same newValue
		}

		// move |chkr| to the NextMutation mutation point
		err = chkr.AdvanceTo(ctx, cur)
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

		newKey, newValue = edits.NextMutation(ctx)
	}

	return chkr.Done(ctx)
}

func equalValues(left, right Item) bool {
	return bytes.Equal(left, right)
}
