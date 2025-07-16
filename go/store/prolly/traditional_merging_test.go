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

package prolly

import (
	"bytes"
	"context"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

func traditionalThreeWayMerge[K ~[]byte, O tree.Ordering[K], S message.Serializer](ctx context.Context, ns tree.NodeStore, left, right, base tree.Node, collide tree.CollisionFn, leftSchemaChange, rightSchemaChange bool, order O, serializer S) (final tree.Node, err error) {
	ld, err := tree.DifferFromRoots[K](ctx, ns, ns, base, left, order, leftSchemaChange)
	if err != nil {
		return tree.Node{}, err
	}

	rd, err := tree.DifferFromRoots[K](ctx, ns, ns, base, right, order, rightSchemaChange)
	if err != nil {
		return tree.Node{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := NewMutationBuffer(1024)
	// iterate |ld| and |rd| in parallel, populating |patches|
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		err = sendPatches(ctx, ld, rd, order, patches, collide)
		return
	})

	// consume |patches| and apply them to |left|
	eg.Go(func() error {
		final, err = tree.ApplyMutations[K](ctx, ns, left, order, serializer, patches)
		return err
	})

	if err = eg.Wait(); err != nil {
		return tree.Node{}, err
	}
	return final, nil
}

func sendPatches[K ~[]byte, O tree.Ordering[K]](ctx context.Context, l, r tree.Differ[K, O], order O, buf MutationBuffer, cb tree.CollisionFn) (err error) {
	var (
		left, right tree.Diff
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
		cmp := order.Compare(ctx, K(left.Key), K(right.Key))

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
			err = buf.SendDiff(ctx, right)
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
			if !bytes.Equal(left.To, right.To) {
				resolved, ok := cb(left, right)
				if ok {
					err = buf.SendDiff(ctx, resolved)
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
		err = buf.SendDiff(ctx, right)
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

// MutationBuffer implements MutationIter. It consumes Diffs
// from the parallel treeDiffers and transforms them into
// patches for the chunker to apply.
type MutationBuffer struct {
	buf chan tree.Mutation
}

var _ tree.MutationIter = MutationBuffer{}

func NewMutationBuffer(sz int) MutationBuffer {
	return MutationBuffer{buf: make(chan tree.Mutation, sz)}
}

func (ps MutationBuffer) SendDiff(ctx context.Context, diff tree.Diff) error {
	p := tree.Mutation{Key: diff.Key, Value: diff.To}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- p:
		return nil
	}
}

func (ps MutationBuffer) SendPatch(ctx context.Context, key, newValue tree.Item) error {
	p := tree.Mutation{Key: key, Value: newValue}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.buf <- p:
		return nil
	}
}

// NextMutation implements MutationIter.
func (ps MutationBuffer) NextMutation(ctx context.Context) tree.Mutation {
	var p tree.Mutation
	select {
	case p = <-ps.buf:
		return p
	case <-ctx.Done():
		return tree.Mutation{}
	}
}

func (ps MutationBuffer) Close() error {
	close(ps.buf)
	return nil
}
