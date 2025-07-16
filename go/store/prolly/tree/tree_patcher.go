// Copyright 2025 Dolthub, Inc.
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

// ApplyPatches iterates over |edits|, applying the patches to |root| to produce a new tree.
func ApplyPatches[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	root Node,
	order O,
	serializer S,
	edits PatchIter,
) (Node, error) {
	newMutation, err := edits.NextPatch(ctx)
	if err != nil {
		return Node{}, err
	}
	// The iterator marks the end of the sequence with a zero object.
	// Since KeyBelowStart is nil for the first chunk, but EndKey should never be nil, we check EndKey to detect when
	// we've reached the end.
	if newMutation.EndKey == nil {
		return root, nil // no mutations
	}

	var cur *cursor
	if newMutation.KeyBelowStart != nil {
		cur, err = newCursorAtKey(ctx, ns, root, K(newMutation.KeyBelowStart), order)
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
			err = applyLeafPatch(ctx, order, chkr, cur, newMutation.EndKey, newMutation.To)
		} else {
			err = applyNodePatch(ctx, order, chkr, cur, K(newMutation.KeyBelowStart), K(newMutation.EndKey), newMutation.To, newMutation.SubtreeCount, newMutation.Level)
		}
		if err != nil {
			return Node{}, err
		}
		prevMutation := newMutation
		newMutation, err = edits.NextPatch(ctx)
		if err != nil {
			return Node{}, err
		}
		nextKey := newMutation.EndKey
		if nextKey == nil {
			break
		} else if prevMutation.EndKey != nil {
			assertTrue(order.Compare(ctx, K(nextKey), K(prevMutation.EndKey)) >= 0, "expected patches to be sorted by key, but got %v before %v", prevMutation, newMutation)
		}
	}

	return chkr.Done(ctx)
}

func applyLeafPatch[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	order O,
	chkr *chunker[S],
	cur *cursor,
	newKey, newValue Item,
) (err error) {
	// move |cur| to the NextPatch mutation point
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

	// move |chkr| to the NextPatch mutation point
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

// applyNodePatch copies every value from a node into a chunker, replacing all other keys in the node's range.
func applyNodePatch[K ~[]byte, O Ordering[K], S message.Serializer](
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
		err = chkr.advanceTo(ctx, cur)

		// The range (fromKey, toKey] is open from below. If there's already something at |fromKey|, advance past it.
		if cur.Valid() && order.Compare(ctx, K(fromKey), K(cur.CurrentKey())) == 0 {
			err = chkr.AddPair(ctx, cur.CurrentKey(), cur.currentValue())
			if err != nil {
				return err
			}
		}
	}

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

	// TODO: Add SeekPast / AdvancePast functions
	err = Seek(ctx, chkr.cur, K(toKey), order)
	if err != nil {
		return err
	}
	if chkr.cur.Valid() && order.Compare(ctx, K(toKey), K(chkr.cur.CurrentKey())) == 0 {
		err = chkr.skip(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
