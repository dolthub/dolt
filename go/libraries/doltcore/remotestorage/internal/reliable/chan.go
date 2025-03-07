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
	"github.com/dolthub/dolt/go/libraries/utils/circular"
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
	// All unack'd |T|s are stored in |buff|. As they get Ackd, they get popped from here.
	buff *circular.Buff[T]

	// We return new |T|s from here and they go into |buff| to be delivered
	// to |output|, possibly multiple times if |Reset| is called before
	// they are Ackd. This is the |src| channel.
	input chan T

	// The current destination where we try to deliver new |T|s that have
	// not been delivered since the last |Reset| call. This is a new
	// channel every time |Reset| is called.
	output chan T

	// Request channel to ask |thread| to shutdown.
	done chan struct{}

	// Request channel used to ack the Front |T|. After this succeeds, the
	// Ackd message will never be delivered on |output| again, even after a
	// |Reset|.  The Ackd message should have already been delivered on
	// |output| for this window of |Reset|. It usually indicates an error
	// in logic to call this concurrently with |Reset| or with a receive
	// from a channel returned by |Recv|.
	ack chan struct{}

	// Request channel to reset the current channel. After this succeeds,
	// all unAckd |T|s in |buff| will be delivered to the new |output|
	// channel.
	reset chan struct{}
	// Indicates |thread| is shutdown. All requests made to |thread| also
	// read from this so they can return successfully if |thread| is
	// already shutdown or shutsdown concurrently.

	closed chan struct{}
	// Used by clients to request to peek at the Front of the current
	// |buff|.
	front chan frontReq[T]

	// Used by clients to read the current value of |c.output|.
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

// Returns the channel to read from in order to read |T|s that have been
// delivered to |src|.
func (c *Chan[T]) Recv() <-chan T {
	select {
	case o := <-c.outputCh:
		return o
	case <-c.closed:
		return c.output
	}
}

// Peek at the earliest unAckd element in |buff|. For reliable, in-order RPC
// machinery, this will be the Request that corresponds to the most recently
// successfully received Response.
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

// Acknowledge the earlier buffered |T| in |buff|, indicating that a response
// was successfully received and processed for it and it will never need to be
// redriven.
func (c *Chan[T]) Ack() {
	select {
	case c.ack <- struct{}{}:
	case <-c.closed:
	}
}

// Reset the channel so that it will redrive all unacknowledged |T|s into the
// channel returned from |Recv| before it sends any new |T|s that it reads from
// |src|.
func (c *Chan[T]) Reset() {
	select {
	case c.reset <- struct{}{}:
	case <-c.closed:
	}
}

// Shutdown the Chan and cleanup any resources associated with it.
func (c *Chan[T]) Close() {
	close(c.done)
	<-c.closed
}

// Chan works on the basis of a request/response actor that run in its own
// goroutine. All the client methods send and receive from channels,
// interacting with this actor thread.
func (c *Chan[T]) thread() {
	input := c.input
	// The current index in |buff| which we are trying to send to |output|.
	// When we reset, this goes back to 0 and we start working through
	// |buff| again.
	outI := 0
	for {
		// |input| gets nil'd out when we see |c.input| close.
		// If |c.input| is closed and our |buff| is empty (all buffered
		// messages have been Ackd), then we are done and we shut down
		// here.
		if input == nil && c.buff.Len() == 0 {
			return
		}

		// We only read a new element from |input| when we don't have
		// anything buffered we're trying to send to |output|.

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
				// We can close |c.output| here because, since
				// we are in this branch, we know we had
				// nothing left to try to send to |output|. If
				// |Reset| gets called, we will get a new
				// output channel, and the buffer will be sent
				// to it. We will make another read from
				// |c.input|, and we will see the closed
				// channel again, and we will close |c.output|
				// again.
				close(c.output)
			} else {
				// Add the newly read |T| to our buffer.
				c.buff.Push(t)
			}
		case thisOutput <- toOut:
			// We successfully delivered the current element in |buff|.
			// The index in |buff| which we will try to deliver next is |outI + 1|.
			outI += 1
		case <-c.reset:
			input = c.input
			c.output = make(chan T)
			outI = 0
		case <-c.ack:
			// Remove the front element from |buff| and decrement |outI|, since |buff| is one element shorter now.
			c.buff.Pop()
			outI -= 1
		case req := <-c.front:
			// Peek at the front of the buffer. Return the element on |resCh|.
			req.resCh <- c.buff.Front()
		}
	}
}
