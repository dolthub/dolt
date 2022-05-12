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

const patchBufferSize = 1024

// CollisionFn is a callback that handles 3-way merging of NodeItems.
// A typical implementation will attempt a cell-wise merge of the tuples,
// or register a conflict if such a merge is not possible.
type CollisionFn func(left, right Diff) (Diff, bool)

// ThreeWayMerge implements a three-way merge algorithm using |base| as the common ancestor, |right| as
// the source branch, and |left| as the destination branch. Both |left| and |right| are diff'd against
// |base| to compute merge patches, but rather than applying both sets of patches to |base|, patches from
// |right| are applied directly to |left|. This reduces the amount of write work and improves performance.
// In the case that a key-value pair was modified on both |left| and |right| with different resulting
// values, the CollisionFn is called to perform a cell-wise merge, or to throw a conflict.
func ThreeWayMerge[S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	left, right, base Node,
	compare CompareFn,
	collide CollisionFn,
	serializer S,
) (final Node, err error) {

	ld, err := DifferFromRoots(ctx, ns, base, left, compare)
	if err != nil {
		return Node{}, err
	}

	rd, err := DifferFromRoots(ctx, ns, base, right, compare)
	if err != nil {
		return Node{}, err
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
		err = sendPatches(ctx, ld, rd, patches, collide)
		return
	})

	// consume |patches| and apply them to |left|
	eg.Go(func() error {
		final, err = ApplyMutations(ctx, ns, left, serializer, patches, compare)
		return err
	})

	if err = eg.Wait(); err != nil {
		return Node{}, err
	}

	return final, nil
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

func sendPatches(ctx context.Context, l, r Differ, buf patchBuffer, cb CollisionFn) (err error) {
	var (
		left, right Diff
		lok, rok    = true, true
	)

	left, err = l.Next(ctx)
	if err == io.EOF {
		err, lok = nil, false
	}
	if err != nil {
		return err
	}

	right, err = r.Next(ctx)
	if err == io.EOF {
		err, rok = nil, false
	}
	if err != nil {
		return err
	}

	for lok && rok {
		cmp := compareDiffKeys(left, right, l.cmp)

		switch {
		case cmp < 0:
			// already in left
			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return err
			}

		case cmp > 0:
			err = buf.sendPatch(ctx, right)
			if err != nil {
				return err
			}

			right, err = r.Next(ctx)
			if err == io.EOF {
				err, rok = nil, false
			}
			if err != nil {
				return err
			}

		case cmp == 0:
			if !equalDiffVals(left, right) {
				resolved, ok := cb(left, right)
				if ok {
					err = buf.sendPatch(ctx, resolved)
				}
				if err != nil {
					return err
				}
			}

			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return err
			}

			right, err = r.Next(ctx)
			if err == io.EOF {
				err, rok = nil, false
			}
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
		err = buf.sendPatch(ctx, right)
		if err != nil {
			return err
		}

		right, err = r.Next(ctx)
		if err == io.EOF {
			err, rok = nil, false
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func compareDiffKeys(left, right Diff, cmp CompareFn) int {
	return cmp(Item(left.Key), Item(right.Key))
}

func equalDiffVals(left, right Diff) bool {
	// todo(andy): bytes must be comparable
	ok := left.Type == right.Type
	return ok && bytes.Equal(left.To, right.To)
}
