package termprogress

import (
	"sync"
	"sync/atomic"
)

// suspended is a nesting counter for terminal-control output suspension.
// When suspended > 0, callers should avoid emitting ephemeral UI output
// such as spinners that use backspaces / redraw.
var suspended int32

// Suspended reports whether terminal-control output is currently suspended.
func Suspended() bool {
	return atomic.LoadInt32(&suspended) > 0
}

// Suspend increments the suspension counter and returns a resume function.
// Callers should typically use: `resume := Suspend(); defer resume()`.
//
// Suspend is safe to call multiple times (nested suspends). The returned resume
// function is idempotent.
func Suspend() (resume func()) {
	atomic.AddInt32(&suspended, 1)
	var once sync.Once
	return func() {
		once.Do(func() {
			atomic.AddInt32(&suspended, -1)
		})
	}
}
