// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
)

// BufferedTreeIter fowards scans a map using a readahead buffer.
type BufferedTreeIter[K, V ~[]byte] struct {
	outCh chan Node
	eg    *errgroup.Group
	close chan struct{}

	curNode Node
	curIdx  int
}

func (t StaticMap[K, V, O]) BufferedIterAll(ctx context.Context, batchSize int) (*BufferedTreeIter[K, V], error) {
	eg, ctx := errgroup.WithContext(ctx)

	c, err := newCursorAtStart(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	s, err := newCursorPastEnd(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	stop := func(curr *cursor) bool {
		return curr.compare(s) >= 0
	}

	if stop(c) {
		// empty range
		ret := &BufferedTreeIter[K, V]{eg: eg, outCh: make(chan Node)}
		close(ret.outCh)
		return ret, nil
	}

	ret := &BufferedTreeIter[K, V]{
		outCh: make(chan Node, batchSize),
		close: make(chan struct{}),
		eg:    eg,
	}

	if c.parent == nil {
		ret.outCh <- c.nd
		close(ret.outCh)
	} else {
		c = c.parent
		s = s.parent
		eg.Go(func() error { return ret.produce(ctx, c, stop) })
	}

	return ret, nil
}

func (b *BufferedTreeIter[K, V]) produce(ctx context.Context, c *cursor, stop func(*cursor) bool) error {
	for {
		gets := make(hash.HashSlice, c.nd.Count())
		for i := 0; i < c.nd.Count(); i++ {
			gets[i] = c.nd.getAddress(i)
		}

		nodes, err := c.nrw.ReadMany(ctx, gets)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			select {
			case b.outCh <- n:
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-b.close:
				return nil
			}
		}

		c.invalidateAtEnd()
		c.advance(ctx)
		if stop(c) {
			close(b.outCh)
			return nil
		}
	}
}

func (b *BufferedTreeIter[K, V]) Next(ctx context.Context) (K, V, error) {
	// in order:
	//  (1) exhaust |it.curNode| key/value pairs
	//  (2) pull new nodes from |it.buffer|
	//  (3) stop when last node is exhausted & buffer drained
	for {
		if !b.curNode.empty() && b.curIdx < b.curNode.Count() {
			key := b.curNode.GetKey(b.curIdx)
			val := b.curNode.GetValue(b.curIdx)
			b.curIdx++
			return K(key), V(val), nil
		}

		select {
		case node, ok := <-b.outCh:
			if !ok {
				err := b.eg.Wait()
				if err != nil {
					return nil, nil, err
				}
				return nil, nil, io.EOF
			}
			b.curIdx = 0
			b.curNode = node
		case <-ctx.Done():
			return nil, nil, context.Cause(ctx)
		case <-b.close:
			return nil, nil, fmt.Errorf("don't read from a closed cursor")
		}
	}
}

func (b *BufferedTreeIter[K, V]) Close() error {
	close(b.close)
	return b.eg.Wait()
}
