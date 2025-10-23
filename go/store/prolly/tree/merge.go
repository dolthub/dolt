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

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

const PatchBufferSize = 1024

// CollisionFn is a callback that handles 3-way merging of NodeItems when any
// key collision occurs. A typical implementation will attempt a cell-wise merge
// of the tuples, or register a conflict if such a merge is not possible.
type CollisionFn func(left, right Diff) (Diff, bool)

type MergeStats struct {
	Adds          int
	Modifications int
	Removes       int
}

// getNextAndSplitIfAtEnd fetches the next Patch from |patchGenerator|, but it avoids emitting a range patch for the
// very last node in a level. This is because those nodes don't represent natural chunk boundaries and thus aren't
// valid patches.
func getNextAndSplitIfAtEnd[K ~[]byte, O Ordering[K]](ctx context.Context, patchGenerator *PatchGenerator[K, O]) (patch Patch, diffType DiffType, isMore bool, err error) {
	patch, diffType, isMore, err = patchGenerator.Next(ctx)
	if err != nil {
		return Patch{}, NoDiff, false, err
	}
	for patchGenerator.to.atEnd() && patch.Level > 0 && diffType != RemovedDiff {
		patch, diffType, isMore, err = patchGenerator.split(ctx)
		if err != nil || !isMore {
			return Patch{}, NoDiff, false, err
		}
	}
	return patch, diffType, isMore, nil
}

// ThreeWayMerge implements a three-way merge algorithm using |base| as the common ancestor, |right| as
// the source branch, and |left| as the destination branch. Both |left| and |right| are diff'd against
// |base| to compute merge patches, but rather than applying both sets of patches to |base|, patches from
// |right| are applied directly to |left|. This reduces the amount of write work and improves performance.
// In the case that a key-value pair was modified on both |left| and |right| with different resulting
// values, the CollisionFn is called to perform a cell-wise merge, or to throw a conflict.
func ThreeWayMerge[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	left, right, base Node,
	collide CollisionFn,
	order O,
	serializer S,
) (final Node, stats MergeStats, err error) {
	ld, err := PatchGeneratorFromRoots[K](ctx, ns, ns, base, left, order)
	if err != nil {
		return Node{}, MergeStats{}, err
	}
	rd, err := PatchGeneratorFromRoots[K](ctx, ns, ns, base, right, order)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := NewPatchBuffer(PatchBufferSize)

	// iterate |ld| and |rd| in parallel, populating |patches|
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		err = SendPatches(ctx, ld, rd, patches, collide)
		return
	})

	// consume |patches| and apply them to |left|
	eg.Go(func() error {
		final, err = ApplyPatches[K](ctx, ns, left, order, serializer, patches)
		return err
	})

	if err = eg.Wait(); err != nil {
		return Node{}, MergeStats{}, err
	}

	return final, stats, nil
}

// compareWithNilAsMin compares two keys, treating nil as below all other values
func compareWithNilAsMin[K ~[]byte, O Ordering[K]](ctx context.Context, order O, left, right K) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return order.Compare(ctx, left, right)
}

// getLevel returns the level that a patch generator is currently emitting patches for.
// usually this is the level of the |to| cursor, but if that cursor is exhausted,
// then the patch generator is emitting removed diffs on the level of the |from| cursor.
func (d *PatchGenerator[K, O]) getLevel() (uint64, error) {
	if d.to.Valid() {
		return d.to.level()
	}
	return d.from.level()
}

// resolveCollision takes two point Patches with the same key and resolves the changes.
func resolveCollision(left Patch, lDiffType DiffType, right Patch, rDiffType DiffType, cb CollisionFn) (merged Patch, ok bool) {
	leftDiff := Diff{
		Key:  left.EndKey,
		From: left.From,
		To:   left.To,
		Type: lDiffType,
	}
	rightDiff := Diff{
		Key:  right.EndKey,
		From: right.From,
		To:   right.To,
		Type: rDiffType,
	}
	resolved, ok := cb(leftDiff, rightDiff)
	return Patch{
		From:   resolved.From,
		EndKey: resolved.Key,
		To:     resolved.To,
		Level:  0,
	}, ok
}

// SendPatches iterates over |l| and |r| in parallel, sending an ordered non-overlapping series of patches into |buf|.
func SendPatches[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	l, r PatchGenerator[K, O],
	buf PatchBuffer,
	cb CollisionFn,
) (err error) {
	var (
		left, right          Patch
		lDiffType, rDiffType DiffType
		lok, rok             = true, true
	)

	left, lDiffType, lok, err = l.Next(ctx)
	if err != nil {
		return err
	}

	right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
	if err != nil {
		return err
	}

	order := l.order
	for lok && rok {
		// If they're ranges, compare the start points, see if they overlap.
		leftLevel, _ := l.getLevel()
		rightLevel, _ := r.getLevel()
		if leftLevel > 0 && rightLevel > 0 {
			if compareWithNilAsMin(ctx, order, K(left.EndKey), K(right.KeyBelowStart)) <= 0 {
				// Left change is entirely before right change.
				// This change is already on the left map, so we ignore it.
				left, lDiffType, lok, err = l.Next(ctx)
				if err != nil {
					return err
				}
			} else if compareWithNilAsMin(ctx, order, K(right.EndKey), K(left.KeyBelowStart)) <= 0 {
				// Right change is entirely before left change.
				err = buf.SendPatch(ctx, right)
				if err != nil {
					return err
				}

				right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err != nil {
					return err
				}
			} else if bytes.Equal(left.To, right.To) {
				// Since these are both at level > 0, this means that both patches contain an address pointing to
				// the same content-addressed chunk.
				// This necessarily means that their end keys are the same. But if one side has added or removed
				// an entire chunk that immediately preceeds this chunk, then their start keys may differ.
				// If the left side added or removed a chunk, we can safely ignore it. If the right side added a chunk,
				// then we already encountered it. But if the right side removed a chunk, we need to emit a patch here
				// that reflects that.
				if compareWithNilAsMin(ctx, order, K(left.KeyBelowStart), K(right.KeyBelowStart)) > 0 {
					err = buf.SendPatch(ctx, right)
					if err != nil {
						return err
					}
				}
				// This change is already on the left map, so we ignore it.
				left, lDiffType, lok, err = l.Next(ctx)
				if err != nil {
					return err
				}

				right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err != nil {
					return err
				}
			} else {
				// In all other cases there's a conflict and we have to split whichever one comes first.
				// If both have the same start key, split both.
				cmp := compareWithNilAsMin(ctx, order, K(left.KeyBelowStart), K(right.KeyBelowStart))
				if cmp <= 0 {
					left, lDiffType, lok, err = l.split(ctx)
					if err != nil {
						return err
					}
				}
				if cmp >= 0 {
					right, rDiffType, rok, err = r.split(ctx)
					if err != nil {
						return err
					}
				}
			}
			continue
		}

		// If one branch returns a range patch and the other returns a point patch, we need to see if they overlap and possibly split the range diff.
		if rightLevel > 0 {
			if compareWithNilAsMin(ctx, order, K(left.EndKey), K(right.KeyBelowStart)) <= 0 {
				// point update comes first
				// This change is already on the left map, so we ignore it.
				left, lDiffType, lok, err = l.Next(ctx)
				if err != nil {
					return err
				}
			} else if order.Compare(ctx, K(left.EndKey), K(right.EndKey)) > 0 {
				// range update comes first
				err = buf.SendPatch(ctx, right)
				if err != nil {
					return err
				}
				right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err != nil {
					return err
				}
			} else {
				// overlap, we need to split the range
				right, rDiffType, rok, err = r.split(ctx)
				if err != nil {
					return err
				}
			}
			continue
		}

		if leftLevel > 0 {
			if compareWithNilAsMin(ctx, order, K(right.EndKey), K(left.KeyBelowStart)) <= 0 {
				// point update comes first
				err = buf.SendPatch(ctx, right)
				if err != nil {
					return err
				}
				right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err != nil {
					return err
				}
			} else if order.Compare(ctx, K(right.EndKey), K(left.EndKey)) > 0 {
				// range update comes first
				// This change is already on the left map, so we ignore it.
				left, lDiffType, lok, err = l.Next(ctx)
				if err != nil {
					return err
				}
			} else {
				// overlap, we need to split the range
				left, lDiffType, lok, err = l.split(ctx)
				if err != nil {
					return err
				}
			}
			continue
		}

		cmp := order.Compare(ctx, K(left.EndKey), K(right.EndKey))

		switch {
		case cmp < 0:
			// This change is already on the left map, so we ignore it.
			left, lDiffType, lok, err = l.Next(ctx)
			if err != nil {
				return err
			}

		case cmp > 0:
			err = buf.SendPatch(ctx, right)
			if err != nil {
				return err
			}

			right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
			if err != nil {
				return err
			}

		case cmp == 0:
			// Convergent edit:
			if !bytes.Equal(left.To, right.To) {
				resolvedPatch, ok := resolveCollision(left, lDiffType, right, rDiffType, cb)
				// If the collision can be resolved, we record it as a patch.
				// Otherwise, the callback function records the conflict and we don't have to do anything here.
				if ok {
					err = buf.SendPatch(ctx, resolvedPatch)
					if err != nil {
						return err
					}
				}
			}

			left, lDiffType, lok, err = l.Next(ctx)
			if err != nil {
				return err
			}

			right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
			if err != nil {
				return err
			}
		}
	}

	if lok {
		// already in left
		return nil
	}

	for rok {
		err = buf.SendPatch(ctx, right)
		if err != nil {
			return err
		}

		right, rDiffType, rok, err = getNextAndSplitIfAtEnd(ctx, &r)
		if err != nil {
			return err
		}
	}

	return nil
}
