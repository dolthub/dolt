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
	"errors"
	"io"
)

// BufferedTreeIter performs a tablescan while reading |batch|
// nodes ahead of the current iterator node into a background buf.
type BufferedTreeIter[K, V ~[]byte] struct {
	// current tuple location
	curr *cursor

	// the function called to moved |curr| forward in the direction of iteration.
	step func(context.Context) error
	// should return |true| if the passed in cursor is past the iteration's stopping point.
	stop func(*cursor) bool

	curNode Node
	curIdx  int

	buf    chan Node
	closed bool

	ctx       context.Context
	cancelCtx context.CancelCauseFunc
}

func (t StaticMap[K, V, O]) BufferedIterAll(ctx context.Context, batchSize int) (*BufferedTreeIter[K, V], error) {
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
		return &BufferedTreeIter[K, V]{curr: nil, closed: true}, nil
	}

	bufCtx, cancel := context.WithCancelCause(ctx)
	bi := &BufferedTreeIter[K, V]{curr: c, stop: stop, step: c.advance, buf: make(chan Node, batchSize), ctx: bufCtx, cancelCtx: cancel}

	go bi.lookaheadThread(ctx)

	return bi, nil
}

// lookaheadThread reads leaf node chunks into |it.buffer|.
func (it *BufferedTreeIter[K, V]) lookaheadThread(ctx context.Context) {
	var err error
	defer it.cancelCtx(err)

	for {
		select {
		case <-it.ctx.Done():
			// something killed our context
			return
		default:
		}

		it.buf <- it.curr.nd

		it.curr.skipToNodeEnd()
		it.curr.advance(ctx)

		if it.stop(it.curr) {
			// only nil error return, sets ctx.Err() = context.ContextCanceled
			return
		}

		if err = it.curr.parent.advance(ctx); err != nil {
			return
		}

		if err = it.curr.fetchNode(ctx); err != nil {
			return
		}
	}
}

func (it *BufferedTreeIter[K, V]) Next(ctx context.Context) (K, V, error) {
	if it.closed {
		return nil, nil, io.EOF
	}
	var err error
	for {
		// in order:
		//  (1) exhaust |it.curNode| key/value pairs
		//  (2) pull new nodes from |it.buffer|
		//  (3) stop when last node is exhausted & buffer drained
		if it.curIdx < it.curNode.Count() {
			key := it.curNode.GetKey(it.curIdx)
			val := it.curNode.GetValue(it.curIdx)
			it.curIdx++
			return K(key), V(val), nil
		}

		select {
		case nd := <-it.buf:
			// expected case, read new node from buffer
			it.curNode = nd
			it.curIdx = 0
			continue
		case <-ctx.Done():
			// execution context failed elsewhere
			err = ctx.Err()
		default:
			// |buffer| is empty. Either:
			//   (1) the buffer is slow to fill and we should wait
			//       until we can read next node, or
			//   (2) we've read all nodes, drained the buffer, and should
			//       now finalize
			select {
			case <-it.ctx.Done():
				err = it.ctx.Err()
				if errors.Is(err, context.Canceled) {
					// happy path drained & finalize
					err = io.EOF
				}
			default:
				continue
			}
		}
		defer it.Close()
		return nil, nil, err
	}
}

func (it *BufferedTreeIter[K, V]) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.cancelCtx(nil)
	close(it.buf)
}

func (it *BufferedTreeIter[K, V]) Current() (key K, value V) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.Valid() {
		k, v := currentCursorItems(it.curr)
		key, value = K(k), V(v)
	}
	return
}
