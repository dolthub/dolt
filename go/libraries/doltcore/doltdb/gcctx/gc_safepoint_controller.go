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

	"github.com/dolthub/dolt/go/store/hash"
)

type GCSafepointController struct {
	mu sync.Mutex
	// All known sessions. The first command registers the session
	// here and SessionEnd causes it to be removed.
	sessions map[GCRootsProvider]*GCSafepointSessionState
}

// A GCRootsProvider is the thing that a GCSafepointController
// tracks. It also calls it a |session|, as it represents a client
// interacting with the database.  The GCRootsProvider is registered
// with the Controller through the first SessionCommandBegin() call,
// and it goes through lifecycle callbacks which effect safepoint
// establishment when it gets passed to SessionEnd and
// SessionCommandEnd as well.
//
// A GCRootsProvider implements a single method |VisitGCRoots|, which
// will be called if a GC needs to establish a safepoint while the
// session is alive. The method is responsible for passing every live
// in-memory chunk address related to the given |db| to the provided
// |roots| callback. If it cannot do this for some reason, it should
// return an error, but in that case the GC will fail.
// |VisitGCRoots| is called while the session is outside a command, or
// while a GC initiated by that session has made it visitable through
// Waiter. If there is an outstanding visit when SessionCommandBegin is
// called, the new command blocks until the visit completes.
type GCRootsProvider interface {
	VisitGCRoots(ctx context.Context, db string, roots func(hash.Hash) bool) error
}

type GCSafepointSessionState struct {
	// All fields are protected by the controller mutex.
	outstandingCommand bool
	pending            map[*GCSafepointWaiter]struct{}
	activeVisits       int
	visitsDone         chan struct{} // closed when activeVisits reaches zero
	gcWaiters          int           // own GC calls that allow visits during this command
}

type GCSafepointWaiter struct {
	controller *GCSafepointController
	ctx        context.Context
	visit      func(context.Context, GCRootsProvider) error
	selfState  *GCSafepointSessionState
	release    sync.Once
	// The remaining count and err are protected by controller.mu. done is closed
	// when every visit has completed or been removed by cancellation.
	remaining int
	done      chan struct{}
	err       error
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
// Multiple waiters may be outstanding at once. Each waiter visits the sessions known when it was
// created, independently of the other waiters. The caller must have finished changing thisSession's
// roots before calling Waiter: until Wait returns, other waiters may visit thisSession even though
// its command is still open. This lets concurrent GC calls visit each other's sessions.
func (c *GCSafepointController) Waiter(ctx context.Context, thisSession GCRootsProvider, visitQuiescedSession func(context.Context, GCRootsProvider) error) *GCSafepointWaiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := &GCSafepointWaiter{controller: c, ctx: ctx, visit: visitQuiescedSession, done: make(chan struct{})}
	if state := c.sessions[thisSession]; state != nil && state.outstandingCommand {
		ret.selfState = state
		state.gcWaiters++
		// This session may itself be blocking another GC. Its pending visits
		// can now proceed without waiting for this command to end.
		for w := range state.pending {
			c.startVisit(state, thisSession, w)
			delete(state.pending, w)
		}
	}
	for sess, state := range c.sessions {
		if sess == thisSession {
			continue
		}
		ret.remaining++
		if state.outstandingCommand && state.gcWaiters == 0 {
			if state.pending == nil {
				state.pending = make(map[*GCSafepointWaiter]struct{})
			}
			state.pending[ret] = struct{}{}
		} else {
			c.startVisit(state, sess, ret)
		}
	}
	if ret.remaining == 0 {
		close(ret.done)
	}
	return ret
}

// startVisit is called with c.mu held. Counting the visit before starting the goroutine
// prevents a new command from running while any waiter is visiting the session.
func (c *GCSafepointController) startVisit(state *GCSafepointSessionState, sess GCRootsProvider, w *GCSafepointWaiter) {
	if state.activeVisits == 0 {
		state.visitsDone = make(chan struct{})
	}
	state.activeVisits++
	go func() {
		err := w.visit(w.ctx, sess)
		c.mu.Lock()
		w.err = errors.Join(w.err, err)
		state.activeVisits--
		if state.activeVisits == 0 {
			close(state.visitsDone)
		}
		w.remaining--
		if w.remaining == 0 {
			close(w.done)
		}
		c.mu.Unlock()
	}()
}

// |Wait| blocks until all of the waiter's visits are complete. A successful
// return from this method signals that all sessions that were known
// about when the waiter was created have been visited by the
// |visitQuiescedSession| callback that was given to |Waiter|.
//
// If ctx is canceled, visits waiting for a command to end are removed.
// Wait returns with an error after any visits already started complete.
//
// In addition to returning an error if the passed in |ctx|
// is |Done| before the wait is finished, this function also
// returns accumulated errors as seen from each
// |visitQuiescedSession| callback. No attempt is made to
// cancel callbacks or to return early in the case that errors
// are seen from the callback functions.
func (w *GCSafepointWaiter) Wait(ctx context.Context) error {
	select {
	case <-w.done:
		w.releaseSelf()
		return w.err
	case <-ctx.Done():
		w.controller.mu.Lock()
		for _, state := range w.controller.sessions {
			if _, ok := state.pending[w]; ok {
				delete(state.pending, w)
				w.remaining--
				if w.remaining == 0 {
					close(w.done)
				}
			}
		}
		w.controller.mu.Unlock()
		// Visits already started cannot be canceled.
		<-w.done
		w.releaseSelf()
		return errors.Join(context.Cause(ctx), w.err)
	}
}

// releaseSelf ends the period in which other GCs can visit this waiter's
// session. Visits started during that period must finish before its command
// can resume. Only the final own waiter closes the period.
func (w *GCSafepointWaiter) releaseSelf() {
	w.release.Do(func() {
		if w.selfState == nil {
			return
		}
		c := w.controller
		c.mu.Lock()
		w.selfState.gcWaiters--
		var visitsDone chan struct{}
		if w.selfState.gcWaiters == 0 && w.selfState.activeVisits > 0 {
			visitsDone = w.selfState.visitsDone
		}
		c.mu.Unlock()
		if visitsDone != nil {
			<-visitsDone
		}
	})
}

// SessionCommandBegin registers a new session and waits for all active visits
// before allowing a new command to access its roots. It must not hold c.mu
// while waiting, since visits need that mutex to finish.
func (c *GCSafepointController) SessionCommandBegin(s GCRootsProvider) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var state *GCSafepointSessionState
	if state = c.sessions[s]; state == nil {
		state = &GCSafepointSessionState{}
		c.sessions[s] = state
	}
	for state.activeVisits > 0 {
		ch := state.visitsDone
		c.mu.Unlock()
		<-ch
		c.mu.Lock()
	}
	if state.outstandingCommand {
		panic("SessionCommandBegin called on a session that already had an outstanding command.")
	}
	state.outstandingCommand = true
	return nil
}

// Called as part of valctx context validation, this asserts that the
// session is registered with an open command.
func (c *GCSafepointController) Validate(s GCRootsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.sessions[s]; state == nil {
		panic("GCSafepointController.Validate; expected session with an open command, but no session registered with controller.")
	} else if !state.outstandingCommand {
		panic("GCSafepointController.Validate; expected session with an open command, but the registered session has OutstandingCommand == false.")
	}
}

// SessionCommandEnd starts every visit registered while the command was running.
func (c *GCSafepointController) SessionCommandEnd(s GCRootsProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.sessions[s]
	if state == nil {
		panic("SessionCommandEnd called on a session that was not registered")
	}
	if !state.outstandingCommand {
		panic("SessionCommandEnd called on a session that did not have an outstanding command.")
	}
	for w := range state.pending {
		c.startVisit(state, s, w)
		delete(state.pending, w)
	}
	state.outstandingCommand = false
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
		if state.outstandingCommand {
			panic("SessionEnd called on a session that had an outstanding command.")
		}
		delete(c.sessions, s)
	}
}
