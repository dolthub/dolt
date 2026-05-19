// Copyright 2026 Dolthub, Inc.
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
	"sync"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
)

// SessionFactory produces a fresh DoltSession for an external session id when
// the registry has no entry yet. The id is passed in so factories can stamp it
// onto the session for logging.
type SessionFactory func(id string) (*DoltSession, error)

// SessionRegistry maps an externally-managed session id (for example a
// MongoDB lsid) onto a DoltSession. It exists to decouple session lifetime
// from a single SQL connection's lifetime: the same logical session can span
// multiple TCP connections and survive idle periods short of the configured
// timeout.
//
// Concurrency: all exported methods are safe for concurrent use. The registry
// does not serialise commands issued against a single session; if two
// concurrent commands could land on the same id, the caller must serialise
// them.
//
// Reconnect: a TCP frame that carries a known id reuses the same DoltSession,
// preserving any in-progress DoltTransaction and per-branch working set
// state.
//
// Timeout: Sweep removes entries whose lastUsed timestamp is older than
// timeout. The reaper schedule is the caller's responsibility -- the registry
// only provides the predicate.
//
// GC safepoint: when a non-nil GCSafepointController is configured, the
// registry hands every newly created DoltSession to the controller via
// SessionCommandBegin and pairs that with SessionEnd at removal time. This
// keeps the GC from reaping chunks pinned by the session's working sets while
// the session is idle in the registry.
type SessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]*registeredSession
	timeout  time.Duration
	factory  SessionFactory
	gc       *gcctx.GCSafepointController
	now      func() time.Time
}

type registeredSession struct {
	sess     *DoltSession
	lastUsed time.Time
}

// NewSessionRegistry returns a registry that creates sessions via factory and
// considers entries older than timeout eligible for Sweep.
func NewSessionRegistry(timeout time.Duration, factory SessionFactory) *SessionRegistry {
	return &SessionRegistry{
		sessions: make(map[string]*registeredSession),
		timeout:  timeout,
		factory:  factory,
		now:      time.Now,
	}
}

// WithGCSafepointController wires a controller into the registry; every
// session created from this point on is registered with the controller, and
// every End / Sweep removal calls SessionEnd on it.
func (r *SessionRegistry) WithGCSafepointController(c *gcctx.GCSafepointController) *SessionRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gc = c
	return r
}

// WithClock overrides the registry's notion of "now". Tests use this to
// advance time deterministically.
func (r *SessionRegistry) WithClock(now func() time.Time) *SessionRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.now = now
	return r
}

// GetOrCreate returns the session for id, creating one via the factory if
// none exists. isNew reports whether a fresh session was minted on this call.
// The session's lastUsed is updated to "now" either way.
func (r *SessionRegistry) GetOrCreate(id string) (sess *DoltSession, isNew bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.sessions[id]; ok {
		entry.lastUsed = r.now()
		return entry.sess, false, nil
	}

	sess, err = r.factory(id)
	if err != nil {
		return nil, false, err
	}
	r.sessions[id] = &registeredSession{sess: sess, lastUsed: r.now()}

	if r.gc != nil {
		// Register the session with the GC controller so its working-set
		// roots are pinned across idle windows. SessionCommandBegin treats
		// "first call" as registration; we pair it immediately with
		// SessionCommandEnd so the session is registered-but-quiesced.
		if begErr := r.gc.SessionCommandBegin(sess); begErr != nil {
			delete(r.sessions, id)
			return nil, false, begErr
		}
		r.gc.SessionCommandEnd(sess)
	}

	return sess, true, nil
}

// Get returns the session for id without creating one. Does not update
// lastUsed -- callers that want to mark activity should call Touch.
func (r *SessionRegistry) Get(id string) (*DoltSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[id]
	if !ok {
		return nil, false
	}
	return entry.sess, true
}

// Touch updates lastUsed for id to "now" if id is known. Returns whether the
// id was found.
func (r *SessionRegistry) Touch(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[id]
	if !ok {
		return false
	}
	entry.lastUsed = r.now()
	return true
}

// End discards the session for id (explicit teardown, e.g. Mongo
// endSessions). Returns whether the id was found.
func (r *SessionRegistry) End(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[id]
	if !ok {
		return false
	}
	delete(r.sessions, id)
	if r.gc != nil {
		r.gc.SessionEnd(entry.sess)
	}
	return true
}

// Sweep removes every session whose lastUsed is older than asOf - timeout.
// Returns the number of sessions removed. Callers schedule Sweep on a timer;
// the registry does not run its own goroutine.
func (r *SessionRegistry) Sweep(asOf time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := asOf.Add(-r.timeout)
	removed := 0
	for id, entry := range r.sessions {
		if entry.lastUsed.Before(cutoff) {
			delete(r.sessions, id)
			if r.gc != nil {
				r.gc.SessionEnd(entry.sess)
			}
			removed++
		}
	}
	return removed
}

// Len reports the number of live sessions. Intended for tests and metrics.
func (r *SessionRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}
