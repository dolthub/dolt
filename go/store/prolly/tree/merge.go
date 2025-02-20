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
	"context"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

const patchBufferSize = 1024

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
	ld, err := DifferFromRoots[K](ctx, ns, ns, base, left, order, leftSchemaChange)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	rd, err := DifferFromRoots[K](ctx, ns, ns, base, right, order, rightSchemaChange)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := newPatchBuffer(patchBufferSize)

	// iterate |ld| and |rd| in parallel, populating |patches|
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		stats, err = sendPatches(ctx, ld, rd, patches, collide)
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

// patchBuffer implements MutationIter. It consumes Diffs
// from the parallel treeDiffers and transforms them into
// patches for the chunker to apply.
type patchBuffer struct {
	buf chan patch
}

var _ MutationIter = patchBuffer{}

type patch [2]Item

func newPatchBuffer(sz int) patchBuffer {
	return patchBuffer{buf: make(chan patch, sz)}
}

func (ps patchBuffer) sendPatch(ctx context.Context, diff Diff) error {
	p := patch{diff.Key, diff.To}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- p:
		return nil
	}
}

// NextMutation implements MutationIter.
func (ps patchBuffer) NextMutation(ctx context.Context) (Item, Item) {
	var p patch
	select {
	case p = <-ps.buf:
		return p[0], p[1]
	case <-ctx.Done():
		return nil, nil
	}
}

func (ps patchBuffer) Close() error {
	close(ps.buf)
	return nil
}

func sendPatches[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	l, r Differ[K, O],
	buf patchBuffer,
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
		cmp := l.order.Compare(ctx, K(left.Key), K(right.Key))

		switch {
		case cmp < 0:
			// already in left
			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return MergeStats{}, err
			}

		case cmp > 0:
			err = buf.sendPatch(ctx, right)
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
			resolved, ok := cb(left, right)
			if ok {
				err = buf.sendPatch(ctx, resolved)
				updateStats(right, &stats)
			}
			if err != nil {
				return MergeStats{}, err
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
		err = buf.sendPatch(ctx, right)
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
