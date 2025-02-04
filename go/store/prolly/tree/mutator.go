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
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

// A Mutation represents a single change being applied to a tree.
// If Level > 0, then To is either an address or nil, and this mutation represents the range (PreviousKey, Key].
// An address indicates that the every row in the provided range should be replaced by the rows in the address.
// A nil address indicates that every row in the provided range has been removed.
// Otherwise, PreviousKey is ignored and should be set to nil.
type Mutation struct {
	PreviousKey, Key Item
	To               Item
	SubtreeCount     uint64
	Level            int
}

type MutationIter interface {
	NextMutation(ctx context.Context) Mutation
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
	newMutation := edits.NextMutation(ctx)
	if newMutation.Key == nil {
		return root, nil // no mutations
	}

	var cur *cursor
	var err error
	if newMutation.PreviousKey != nil {
		cur, err = newCursorAtKey(ctx, ns, root, K(newMutation.Key), order)
	} else {
		// No prior key for node means that this is the very first node in its row.
		cur, err = newCursorAtStart(ctx, ns, root)
	}

	if err != nil {
		return Node{}, err
	}

	chkr, err := newChunker(ctx, cur.clone(), 0, ns, serializer)
	if err != nil {
		return Node{}, err
	}

	for {
		if newMutation.Level == 0 {
			err = applyLeafMutation(ctx, order, chkr, cur, newMutation.Key, newMutation.To)
		} else {
			err = applyNodeMutation(ctx, order, chkr, cur, K(newMutation.PreviousKey), K(newMutation.Key), newMutation.To, newMutation.SubtreeCount, newMutation.Level)
		}
		if err != nil {
			return Node{}, err
		}
		prev := newMutation.Key
		newMutation = edits.NextMutation(ctx)
		nextKey := newMutation.Key
		if nextKey == nil {
			nextKey = newMutation.PreviousKey
		}
		if nextKey == nil {
			break
		} else if prev != nil {
			assertTrue(order.Compare(ctx, K(nextKey), K(prev)) >= 0, "expected sorted edits")
		}
	}

	return chkr.Done(ctx)
}

func applyLeafMutation[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	order O,
	chkr *chunker[S],
	cur *cursor,
	newKey, newValue Item,
) (err error) {
	// move |cur| to the NextMutation mutation point
	err = Seek(ctx, cur, K(newKey), order)
	if err != nil {
		return err
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
			return nil
		}
	}

	if oldValue == nil && newValue == nil {
		// Don't try to delete what isn't there.
		return nil
	}

	// move |chkr| to the NextMutation mutation point
	err = chkr.advanceTo(ctx, cur)
	if err != nil {
		return err
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
	return err
}

// applyNodeMutation copies every value from a node into a chunker, replacing all other keys in the node's range.
func applyNodeMutation[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	order O,
	chkr *chunker[S],
	cur *cursor,
	fromKey K, toKey K, addr []byte, subtree uint64, level int) (err error) {

	// prevKey may be nil if we're in the very first block.
	// |cur| may be invalid if we've exhausted the original tree.
	if fromKey != nil {
		err = Seek(ctx, cur, K(fromKey), order)
		if err != nil {
			return err
		}
		// The range (fromKey, toKey] is open from below. If there's already something at |fromKey|, advance past it.
		if cur.Valid() && order.Compare(ctx, K(fromKey), K(cur.CurrentKey())) == 0 {
			err = cur.advance(ctx)
			if err != nil {
				return err
			}
		}
	}

	err = chkr.advanceTo(ctx, cur)

	if err != nil {
		return err
	}
	// Append all key-values from the Node.
	// If we're on a chunk boundary, this will just copy the node in.

	// If the start of the range is greater than the last key written, and the tree levels line up, we can just write the supplied address.
	// If supplied tree level is *above* our current one, we need to load the chunk and write its children until we line up again.
	// But it might be below, in which case we need to make sure that we write the address at the right level.
	if addr != nil {
		err = insertNode(ctx, chkr, fromKey, toKey, hash.New(addr), subtree, level, order)
		if err != nil {
			return err
		}
	}

	err = Seek(ctx, chkr.cur, K(toKey), order)
	if err != nil {
		return err
	}
	return nil
}

func equalValues(left, right Item) bool {
	return bytes.Equal(left, right)
}
