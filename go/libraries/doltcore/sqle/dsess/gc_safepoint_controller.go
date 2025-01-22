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

package dsess

import (
	"context"
	"sync"
	"sync/atomic"
)

type GCSafepointController struct {
	mu sync.Mutex
	// All known sessions. The first command registers the session
	// here, and SessionEnd causes it to be removed.
	sessions map[*DoltSession]*GCSafepointSessionState
}

type GCSafepointSessionState struct {
	// True when a command is outstanding on the session,
	// false otherwise.
	OutstandingCommand bool

	// Registered when we create a GCSafepointWaiter, this
	// will be called when the session's SessionCommandEnd
	// function is hit.
	CommandEndCallback func()

	// When this channel is non-nil, it means that an
	// outstanding visit session call is ongoing for this
	// session. The CommandBegin callback will block until
	// that call has completed.
	QuiesceCallbackDone atomic.Value // chan struct{}
}

type GCSafepointWaiter struct {
	controller *GCSafepointController
	wg         sync.WaitGroup
}

func NewGCSafepointController() *GCSafepointController {
	return &GCSafepointController{
		sessions: make(map[*DoltSession]*GCSafepointSessionState),
	}
}

// The GCSafepointController is keeping track of *DoltSession instances that have ever had work done.
// By pairing up CommandBegin and CommandEnd callbacks, it can identify quiesced sessions--ones that
// are not currently running work. Calling |Waiter| asks the controller to concurrently call
// |visitQuiescedSession| on each known session as soon as it is safe and possible. The returned
// |Waiter| is used to |Wait| for all of those to be completed. A call is not made for |thisSession|,
// since, if that session corresponds to an ongoing SQL procedure call, for example, that session
// will never quiesce.
//
// After creating a Waiter, it is an error to create a new Waiter before the |Wait| method of the
// original watier has returned. This error is not guaranteed to always be detected.
func (c *GCSafepointController) Waiter(thisSession *DoltSession, visitQuiescedSession func(*DoltSession)) *GCSafepointWaiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := &GCSafepointWaiter{controller: c}
	for sess, state := range c.sessions {
		if state.CommandEndCallback != nil {
			panic("Attempt to create more than one GCSafepointWaiter.")
		}
		if sess == thisSession {
			continue
		}
		if state.OutstandingCommand {
			ret.wg.Add(1)
			state.CommandEndCallback = func() {
				state.QuiesceCallbackDone.Store(make(chan struct{}))
				go func() {
					visitQuiescedSession(sess)
					ret.wg.Done()
					toClose := state.QuiesceCallbackDone.Load().(chan struct{})
					close(toClose)
				}()
			}
		} else {
			ret.wg.Add(1)
			state.QuiesceCallbackDone.Store(make(chan struct{}))
			go func() {
				visitQuiescedSession(sess)
				ret.wg.Done()
				toClose := state.QuiesceCallbackDone.Load().(chan struct{})
				close(toClose)
			}()
		}
	}
	return ret
}

func (w *GCSafepointWaiter) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
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
		return context.Cause(ctx)
	}
}

var closedCh = make(chan struct{})
func init() {
	close(closedCh)
}

func (c *GCSafepointController) SessionCommandBegin(s *DoltSession) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var state *GCSafepointSessionState
	if state = c.sessions[s]; state == nil {
		state = &GCSafepointSessionState{}
		state.QuiesceCallbackDone.Store(closedCh)
		c.sessions[s] = state
	}
	if state.OutstandingCommand {
		panic("SesisonBeginCommand called on a session that already had an outstanding command.")
	}
	toWait := state.QuiesceCallbackDone.Load().(chan struct{})
	select {
	case <-toWait:
	default:
		c.mu.Unlock()
		<-toWait
		c.mu.Lock()
	}
	state.OutstandingCommand = true
	return nil
}

func (c *GCSafepointController) SessionCommandEnd(s *DoltSession) {
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

// Because we only register sessions when the BeginCommand, it is technically
// possible to get a SessionEnd callback for a session that was never registered.
// However, if there is a corresponding session, it is certainly an error for
// us to get this callback and have OutstandingCommand == true.
func (c *GCSafepointController) SessionEnd(s *DoltSession) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.sessions[s]
	if state != nil && state.OutstandingCommand == true {
		panic("SessionEnd called on a session that had an outstanding command.")
	}
	delete(c.sessions, s)
}
