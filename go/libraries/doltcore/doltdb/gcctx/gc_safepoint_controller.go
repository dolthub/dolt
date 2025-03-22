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

package gcctx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dolthub/dolt/go/store/hash"
)

type GCSafepointController struct {
	mu sync.Mutex
	// All known sessions. The first command registers the session
	// here and SessionEnd causes it to be removed.
	sessions map[GCRootsProvider]*GCSafepointSessionState
}

type GCRootsProvider interface {
	VisitGCRoots(ctx context.Context, db string, roots func(hash.Hash) bool) error
}

type GCSafepointSessionState struct {
	// True when a command is outstanding on the session,
	// false otherwise.
	OutstandingCommand bool

	// Registered when we create a GCSafepointWaiter if
	// there is an outstanding command on the session
	// at the time. This will be called when the session's
	// SessionCommandEnd function is called.
	CommandEndCallback func()

	// When this channel is non-nil, it means that an
	// outstanding visit session call is ongoing for this
	// session. The CommandBegin callback will block until
	// that call has completed.
	QuiesceCallbackDone atomic.Value // chan struct{}
}

// Make is so that HasOutstandingVisitCall will return true and
// BlockForOutstandingVisitCall will block until
// EndOutstandingVisitCall is called.
func (state *GCSafepointSessionState) BeginOutstandingVisitCall() {
	state.QuiesceCallbackDone.Store(make(chan struct{}))
}

// Bracket the end of an outstanding visit call. Unblocks any
// callers to |BlockForOutstandingVisitCall|. Must be paired
// one-for-one with calls to |BeginOutstandingVisitCall|.
func (state *GCSafepointSessionState) EndOutstandingVisitCall() {
	close(state.QuiesceCallbackDone.Load().(chan struct{}))
}

// Peek whether |BlockForOutstandingVisitCall| would block.
func (state *GCSafepointSessionState) HasOutstandingVisitCall() bool {
	ch := state.QuiesceCallbackDone.Load().(chan struct{})
	select {
	case <-ch:
		return false
	default:
		return true
	}
}

func (state *GCSafepointSessionState) BlockForOutstandingVisitCall() {
	ch := state.QuiesceCallbackDone.Load().(chan struct{})
	<-ch
}

var closedCh = make(chan struct{})

func init() {
	close(closedCh)
}

func NewGCSafepointSessionState() *GCSafepointSessionState {
	state := &GCSafepointSessionState{}
	state.QuiesceCallbackDone.Store(closedCh)
	return state
}

type GCSafepointWaiter struct {
	controller *GCSafepointController
	wg         sync.WaitGroup
	mu         sync.Mutex
	err        error
}

func NewGCSafepointController() *GCSafepointController {
	return &GCSafepointController{
		sessions: make(map[GCRootsProvider]*GCSafepointSessionState),
	}
}

// The GCSafepointController is keeping track of *DoltSession instances that have ever had work done.
// By pairing up CommandBegin and CommandEnd callbacks, it can identify quiesced sessions--ones that
// are not currently running work. Calling |Waiter| asks the controller to concurrently call
// |visitQuiescedSession| on each known session as soon as it is safe and possible. The returned
// |Waiter| is used to |Wait| for all of those to be completed. A call is not made for |thisSession|,
// since, if that session corresponds to an ongoing SQL procedure call, for example, that session
// will never quiesce. Instead, the caller should ensure that |visitQuiescedSession| is called on
// its own session.
//
// After creating a Waiter, it is an error to create a new Waiter before the |Wait| method of the
// original watier has returned. This error is not guaranteed to always be detected.
func (c *GCSafepointController) Waiter(ctx context.Context, thisSession GCRootsProvider, visitQuiescedSession func(context.Context, GCRootsProvider) error) *GCSafepointWaiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := &GCSafepointWaiter{controller: c}
	for sess, state := range c.sessions {
		// If an existing session already has a |CommandEndCallback| registered,
		// then more than one |Waiter| would be outstanding on this
		// SafepointController. This is an error and is not supported.
		if state.CommandEndCallback != nil {
			panic("Attempt to create more than one GCSafepointWaiter.")
		}
		if sess == thisSession {
			continue
		}
		// When this session's |visit| call is done, it will count down this
		// waitgroup. The |Wait| method, in turn, will block on this waitgroup
		// completing to know that all callback are done.
		ret.wg.Add(1)
		// The work we do when we visit the session, including bookkeeping.
		work := func() {
			// We don't set this until the callback is actually called.
			// If we did set this outside of the callback, Wait's
			// cleanup logic would need to change to ensure that the
			// session is in a usable state when the callback gets
			// canceled before ever being called.
			state.BeginOutstandingVisitCall()
			go func() {
				err := visitQuiescedSession(ctx, sess)
				ret.accumulateErrors(err)
				ret.wg.Done()
				state.EndOutstandingVisitCall()
			}()
		}
		if state.OutstandingCommand {
			// If a command is currently running on the session, register
			// our work to run as soon as the command is done.
			state.CommandEndCallback = work
		} else {
			// When no command is running on the session, we can immediately
			// visit it.
			work()
		}
	}
	return ret
}

func (w *GCSafepointWaiter) accumulateErrors(err error) {
	if err != nil {
		w.mu.Lock()
		w.err = errors.Join(w.err, err)
		w.mu.Unlock()
	}
}

// |Wait| will block on the Waiter's waitgroup. A successful
// return from this method signals that all sessions that were known
// about when the waiter was created have been visited by the
// |visitQuiescedSession| callback that was given to |Waiter|.
//
// This function will return early, and with an error, if the
// supplied |ctx|'s |Done| channel delivers. In that case,
// all sessions will not necessarily have been visited, but
// any visit callbacks which were started will still have
// completed.
//
// In addition to returning an error if the passed in |ctx|
// is |Done| before the wait is finished, this function also
// returns accumulated errors as seen from each
// |visitQuiescedSession| callback. No attempt is made to
// cancel callbacks or to return early in the case that errors
// are seen from the callback functions.
func (w *GCSafepointWaiter) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return w.err
	case <-ctx.Done():
		w.controller.mu.Lock()
		for _, state := range w.controller.sessions {
			if state.CommandEndCallback != nil {
				// Do not visit the session, but do
				// count down the WaitGroup so that
				// the goroutine above still completes.
				w.wg.Done()
				state.CommandEndCallback = nil
			}
		}
		w.controller.mu.Unlock()
		// Once a session visit callback has started, we
		// cannot cancel it. So we wait for all the inflight
		// callbacks to be completed here, before returning.
		<-done
		return errors.Join(context.Cause(ctx), w.err)
	}
}

// Beginning a command on a session has three effects:
//
//  1. It registers the Session in the set of all
//     known sessions, |c.sessions|, if this is our
//     first time seeing it.
//
//  2. It blocks for any existing call to |CommandEndCallback|
//     on this session to complete. If a call to |CommendEndCallback|
//     is outstanding, our |QuiesceCallbackDone| a read from our
//     |QuiesceCallbackDone| channel will block.
//
//  3. It sets |OutstandingCommand| for the Session to true. Only
//     one command can be outstanding at a time, and whether a command
//     is outstanding controls how |Waiter| treats the Session when it
//     is setting up all Sessions to visit their GC roots.
func (c *GCSafepointController) SessionCommandBegin(s GCRootsProvider) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var state *GCSafepointSessionState
	if state = c.sessions[s]; state == nil {
		// Step #1: keep track of all seen sessions.
		state = NewGCSafepointSessionState()
		c.sessions[s] = state
	}
	if state.OutstandingCommand {
		panic("SessionCommandBegin called on a session that already had an outstanding command.")
	}
	// Step #2: Receiving from QuiesceCallbackDone blocks, then
	// the callback for this Session is still outstanding. We
	// don't want to block on this work finishing while holding
	// the controller-wide lock, so we release it while we block.
	if state.HasOutstandingVisitCall() {
		c.mu.Unlock()
		state.BlockForOutstandingVisitCall()
		c.mu.Lock()
		if state.OutstandingCommand {
			// Concurrent calls to SessionCommandBegin. Bad times...
			panic("SessionCommandBegin called on a session that already had an outstanding command.")
		}
	}
	// Step #3. Record that a command is running so that Waiter
	// will populate CommandEndCallback instead of running the
	// visit logic immediately.
	state.OutstandingCommand = true
	return nil
}

// Called as part of valctx context validation, this asserts that the
// session is registered with an open command.
func (c *GCSafepointController) Validate(s GCRootsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.sessions[s]; state == nil {
		panic("GCSafepointController.Validate; expected session with an open command, but no session registered with controller.")
	} else if !state.OutstandingCommand {
		panic("GCSafepointController.Validate; expected session with an open command, but the registered session has OutstandingCommand == false.")
	}
}

// SessionCommandEnd marks the end of a session command. It has for
// effects that the session no longer has an OutstandingCommand and,
// if CommandEndCallback was non-nil, the callback itself has been
// called and the CommandEndCallback field has been reset to |nil|.
func (c *GCSafepointController) SessionCommandEnd(s GCRootsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.sessions[s]
	if state == nil {
		panic("SessionCommandEnd called on a session that was not registered")
	}
	if state.OutstandingCommand != true {
		panic("SessionCommandEnd called on a session that did not have an outstanding command.")
	}
	if state.CommandEndCallback != nil {
		state.CommandEndCallback()
		state.CommandEndCallback = nil
	}
	state.OutstandingCommand = false
}

// SessionEnd will remove the session from our tracked session state,
// if we already knew about it. It is an error to call this on a
// session which currently has an outstanding command.
//
// Because we only register sessions when the CommandBegin, it is
// possible to get a SessionEnd callback for a session that was
// never registered.
//
// This callback does not block for any outstanding |visitQuiescedSession|
// callback to be completed before allowing the session to unregister
// itself. It is an error for the application to call |SessionCommandBegin|
// on a session after it is has called |SessionEnd| on it, but that error
// is not necessarily detected.
func (c *GCSafepointController) SessionEnd(s GCRootsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.sessions[s]
	if state != nil {
		if state.OutstandingCommand == true {
			panic("SessionEnd called on a session that had an outstanding command.")
		}
		delete(c.sessions, s)
	}
}
