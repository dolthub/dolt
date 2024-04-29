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

package reliable

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/circular"
)

// A reliable.Chan is a type of channel transformer which can be used to build
// guaranteed delivery machinery. Constructed with a source channel, it will
// deliver elements it receives from the source channel into its |Recv|
// channel. As the elements are successfully processed by the consumer of
// |Recv|, |Ack| should be called, which ensures that the oldest unacked
// delivery will not be redelivered. However, if the batch of un-Ack'd messages
// needs to be reprocessed, |Reset| should be called. After |Reset| returns,
// the un-|Ack|d messages will be redriven through |Recv| before new messages
// are read from the source channel.
//
// The channel that |Recv| refers to will change after a |Reset|. Chan will
// close |Recv| after its source channel closes and all buffered messages have
// been delivered on |Recv|. In general, reads from then channel returned by
// Recv should not happen concurrently with a call to Reset - the reads and the
// call are safe, but the read is not guaranteed to ever complete.
//
// Close should always be called on an reliable.Chan to ensure resource cleanup.
type Chan[T any] struct {
	buff *circular.Buff[T]

	input  chan T
	output chan T

	done     chan struct{}
	ack      chan struct{}
	reset    chan struct{}
	closed   chan struct{}
	front    chan frontReq[T]
	outputCh chan chan T
}

type frontReq[T any] struct {
	resCh chan T
}

func NewChan[T any](src chan T) *Chan[T] {
	ret := &Chan[T]{
		buff:     circular.NewBuff[T](8),
		input:    src,
		done:     make(chan struct{}),
		ack:      make(chan struct{}),
		reset:    make(chan struct{}),
		closed:   make(chan struct{}),
		front:    make(chan frontReq[T]),
		output:   make(chan T),
		outputCh: make(chan chan T),
	}
	go func() {
		defer close(ret.closed)
		ret.thread()
	}()
	return ret
}

func (c *Chan[T]) Recv() <-chan T {
	select {
	case o := <-c.outputCh:
		return o
	case <-c.closed:
		return c.output
	}
}

func (c *Chan[T]) Front() (T, bool) {
	var res T
	resCh := make(chan T)
	select {
	case c.front <- frontReq[T]{resCh: resCh}:
		return <-resCh, true
	case <-c.closed:
		return res, false
	}
}

func (c *Chan[T]) Ack() {
	select {
	case c.ack <- struct{}{}:
	case <-c.closed:
	}
}

func (c *Chan[T]) Reset() {
	select {
	case c.reset <- struct{}{}:
	case <-c.closed:
	}
}

func (c *Chan[T]) Close() {
	close(c.done)
	<-c.closed
}

func (c *Chan[T]) thread() {
	input := c.input
	outI := 0
	for {
		if input == nil && c.buff.Len() == 0 {
			return
		}

		thisInput := input
		thisOutput := c.output
		var toOut T

		if outI < c.buff.Len() {
			// We have an element we're trying to get out...
			thisInput = nil
			toOut = c.buff.At(outI)
		} else {
			// No element we're trying to send -- read from input.
			thisOutput = nil
		}

		select {
		case <-c.done:
			return
		case c.outputCh <- c.output:
		case t, ok := <-thisInput:
			if !ok {
				input = nil
				close(c.output)
			} else {
				c.buff.Push(t)
			}
		case thisOutput <- toOut:
			outI += 1
		case <-c.reset:
			input = c.input
			c.output = make(chan T)
			outI = 0
		case <-c.ack:
			c.buff.Pop()
			outI -= 1
		case req := <-c.front:
			req.resCh <- c.buff.Front()
		}
	}
}
