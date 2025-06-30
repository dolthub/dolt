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
	"io"

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
	leftSchemaChange, rightSchemaChange bool,
	order O,
	serializer S,
) (final Node, stats MergeStats, err error) {
	ld, err := RangeDifferFromRoots[K](ctx, ns, ns, base, left, order, leftSchemaChange)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	rd, err := RangeDifferFromRoots[K](ctx, ns, ns, base, right, order, rightSchemaChange)
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
		stats, err = SendPatches(ctx, ld, rd, patches, collide)
		return
	})

	// consume |patches| and apply them to |left|
	eg.Go(func() error {
		final, err = ApplyMutations[K](ctx, ns, left, order, serializer, patches)
		return err
	})

	if err = eg.Wait(); err != nil {
		return Node{}, MergeStats{}, err
	}

	return final, stats, nil
}

// PatchBuffer implements MutationIter. It consumes Diffs
// from the parallel treeDiffers and transforms them into
// patches for the chunker to apply.
type PatchBuffer struct {
	buf chan Mutation
}

var _ MutationIter = PatchBuffer{}

func NewPatchBuffer(sz int) PatchBuffer {
	return PatchBuffer{buf: make(chan Mutation, sz)}
}

func (ps PatchBuffer) SendDiff(ctx context.Context, diff Diff) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- diff.Mutation:
		return nil
	}
}

func (ps PatchBuffer) SendKV(ctx context.Context, key, value Item) error {
	patch := Mutation{
		PreviousKey:  nil,
		Key:          key,
		To:           value,
		SubtreeCount: 1,
		Level:        0,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- patch:
		return nil
	}
}

func (ps PatchBuffer) SendDone(ctx context.Context) error {
	patch := Mutation{
		PreviousKey: nil,
		Key:         nil,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- patch:
		return nil
	}
}

// NextMutation implements MutationIter.
func (ps PatchBuffer) NextMutation(ctx context.Context) (mutation Mutation) {
	select {
	case mutation = <-ps.buf:
		return mutation
	case <-ctx.Done():
		return mutation
	}
}

func (ps PatchBuffer) Close() error {
	close(ps.buf)
	return nil
}

// nilCompare compares two keys, treating nil as below all other values
func nilCompare[K ~[]byte, O Ordering[K]](ctx context.Context, order O, left, right K) int {
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

func getNextAndSplitIfAtEnd[K ~[]byte, O Ordering[K]](ctx context.Context, differ *Differ[K, O]) (diff Diff, err error) {
	diff, err = differ.Next(ctx)
	if err != nil {
		return diff, err
	}
	for diff.Type == RangeDiff && differ.to.atEnd() {
		diff, err = differ.split(ctx)
		if err != nil {
			return diff, err
		}
	}
	return diff, nil
}

// getLevel returns the level that a differ is currently emitting diffs for.
// usually this is the level of the |to| cursor, but if that cursor is exhausted,
// then the differ is emitting removed diffs on the level of the |from| cursor.
func getLevel[K ~[]byte, O Ordering[K]](d Differ[K, O]) (uint64, error) {
	if d.to.Valid() {
		return d.to.level()
	}
	return d.from.level()
}

func SendPatches[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	l, r Differ[K, O],
	buf PatchBuffer,
	cb CollisionFn,
) (stats MergeStats, err error) {
	var (
		left, right Diff
		lok, rok    = true, true
	)

	left, err = l.Next(ctx)
	if err == io.EOF {
		err, lok = nil, false
	}
	if err != nil {
		return MergeStats{}, err
	}

	right, err = r.Next(ctx)
	if err == io.EOF {
		err, rok = nil, false
	}
	if err != nil {
		return MergeStats{}, err
	}

	for lok && rok {
		order := l.order
		// If they're ranges, compare the start points, see if they overlap.
		leftLevel, _ := getLevel(l)
		rightLevel, _ := getLevel(r)
		if leftLevel > 0 && rightLevel > 0 {
			if nilCompare(ctx, order, K(left.Key), K(right.PreviousKey)) <= 0 {
				// Left change is entirely before right change.
				// This change is already on the left map, so we ignore it.
				// left, err = l.Next(ctx)
				left, err = getNextAndSplitIfAtEnd(ctx, &l)
				if err == io.EOF {
					err, lok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else if nilCompare(ctx, order, K(right.Key), K(left.PreviousKey)) <= 0 {
				// Right change is entirely before right change.
				err = buf.SendDiff(ctx, right)
				if err != nil {
					return MergeStats{}, err
				}
				updateStats(right, &stats)

				right, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err == io.EOF {
					err, rok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else if bytes.Equal(left.To, right.To) {
				// A concurrent change.
				// This change is already on the left map, so we ignore it.
				left, err = getNextAndSplitIfAtEnd(ctx, &l)
				if err == io.EOF {
					err, lok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}

				// right, err = r.Next(ctx)
				right, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err == io.EOF {
					err, rok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else {
				// In all other cases there's a conflict and we have to split whichever one comes first.
				// If both have the same start key, split both.
				cmp := nilCompare(ctx, order, K(left.PreviousKey), K(right.PreviousKey))
				if cmp <= 0 {
					left, err = l.split(ctx)
					if err != nil {
						return MergeStats{}, err
					}
				}
				if cmp >= 0 {
					right, err = r.split(ctx)
					if err != nil {
						return MergeStats{}, err
					}
				}
			}
			continue
		}

		// If one branch returns a range diff and the other returns a point diff, we need to see if they overlap and possibly split the range diff.
		if rightLevel > 0 {
			if l.order.Compare(ctx, K(left.Key), K(right.PreviousKey)) <= 0 {
				// point update comes first
				// This change is already on the left map, so we ignore it.
				left, err = getNextAndSplitIfAtEnd(ctx, &l)
				if err == io.EOF {
					err, lok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else if l.order.Compare(ctx, K(left.Key), K(right.Key)) > 0 {
				// range update comes first
				err = buf.SendDiff(ctx, right)
				if err != nil {
					return MergeStats{}, err
				}
				updateStats(right, &stats)
				right, err = getNextAndSplitIfAtEnd(ctx, &r)
				if err == io.EOF {
					err, rok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else {
				// overlap, we need to split the range
				right, err = r.split(ctx)
				if err != nil {
					return MergeStats{}, err
				}
			}
			continue
		}

		if leftLevel > 0 {
			if nilCompare(ctx, l.order, K(right.Key), K(left.PreviousKey)) <= 0 {
				// point update comes first
				err = buf.SendDiff(ctx, right)
				if err != nil {
					return MergeStats{}, err
				}
				updateStats(right, &stats)
				right, err = r.Next(ctx)
				if err == io.EOF {
					err, rok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else if nilCompare(ctx, l.order, K(right.Key), K(left.Key)) > 0 {
				// range update comes first
				// This change is already on the left map, so we ignore it.
				left, err = l.Next(ctx)
				if err == io.EOF {
					err, lok = nil, false
				}
				if err != nil {
					return MergeStats{}, err
				}
			} else {
				// overlap, we need to split the range
				left, err = l.split(ctx)
				if err != nil {
					return MergeStats{}, err
				}
			}
			continue
		}

		cmp := l.order.Compare(ctx, K(left.Key), K(right.Key))

		switch {
		case cmp < 0:
			// This change is already on the left map, so we ignore it.
			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return MergeStats{}, err
			}

		case cmp > 0:
			err = buf.SendDiff(ctx, right)
			if err != nil {
				return MergeStats{}, err
			}
			updateStats(right, &stats)

			right, err = r.Next(ctx)
			if err == io.EOF {
				err, rok = nil, false
			}
			if err != nil {
				return MergeStats{}, err
			}

		case cmp == 0:
			// Convergent edit:
			if !bytes.Equal(left.To, right.To) {
				resolved, ok := cb(left, right)
				if ok {
					err = buf.SendDiff(ctx, resolved)
					updateStats(right, &stats)
				}
				if err != nil {
					return MergeStats{}, err
				}
			}

			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return MergeStats{}, err
			}

			right, err = r.Next(ctx)
			if err == io.EOF {
				err, rok = nil, false
			}
			if err != nil {
				return MergeStats{}, err
			}
		}
	}

	if lok {
		// already in left
		return stats, nil
	}

	for rok {
		err = buf.SendDiff(ctx, right)
		if err != nil {
			return MergeStats{}, err
		}
		updateStats(right, &stats)

		right, err = r.Next(ctx)
		if err == io.EOF {
			err, rok = nil, false
		}
		if err != nil {
			return MergeStats{}, err
		}
	}

	return stats, nil
}

func updateStats(right Diff, stats *MergeStats) {
	switch right.Type {
	case AddedDiff:
		stats.Adds++
	case RemovedDiff:
		stats.Removes++
	case ModifiedDiff:
		stats.Modifications++
	}
}
