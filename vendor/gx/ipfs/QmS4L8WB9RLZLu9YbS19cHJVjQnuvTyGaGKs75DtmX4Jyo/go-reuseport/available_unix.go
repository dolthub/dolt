// +build darwin freebsd dragonfly netbsd openbsd linux

package reuseport

import (
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// checker is a struct to gather the availability check fields + funcs.
// we use atomic ints because this is potentially a really hot function call.
type checkerT struct {
	avail int32      // atomic int managed by set/isAvailable()
	check int32      // atomic int managed by has/checked()
	mu    sync.Mutex // synchonizes the actual check
}

// the static location of the vars.
var checker checkerT

func (c *checkerT) isAvailable() bool {
	return atomic.LoadInt32(&c.avail) != 0
}

func (c *checkerT) setIsAvailable(b bool) {
	if b {
		atomic.StoreInt32(&c.avail, 1)
	} else {
		atomic.StoreInt32(&c.avail, 0)
	}
}

func (c *checkerT) hasChecked() bool {
	return atomic.LoadInt32(&c.check) != 0
}

func (c *checkerT) setHasChecked(b bool) {
	if b {
		atomic.StoreInt32(&c.check, 1)
	} else {
		atomic.StoreInt32(&c.check, 0)
	}
}

// Available returns whether or not SO_REUSEPORT is available in the OS.
// It does so by attepting to open a tcp listener, setting the option, and
// checking ENOPROTOOPT on error. After checking, the decision is cached
// for the rest of the process run.
func available() bool {
	if checker.hasChecked() {
		return checker.isAvailable()
	}

	// synchronize, only one should check
	checker.mu.Lock()
	defer checker.mu.Unlock()

	// we blocked. someone may have been gotten this.
	if checker.hasChecked() {
		return checker.isAvailable()
	}

	// there may be fluke reasons to fail to add a listener.
	// so we give it 5 shots. if not, give up and call it not avail.
	for i := 0; i < 5; i++ {
		// try to listen at tcp port 0.
		l, err := listenStream("tcp", "127.0.0.1:0")
		if err == nil {
			// no error? available.
			checker.setIsAvailable(true)
			checker.setHasChecked(true)
			l.Close() // Go back to the Shadow!
			return true
		}

		if errno, ok := err.(syscall.Errno); ok {
			if errno == syscall.ENOPROTOOPT {
				break // :( that's all folks.
			}
		}

		// not an errno? or not ENOPROTOOPT? retry.
		<-time.After(20 * time.Millisecond) // wait a bit
	}

	checker.setIsAvailable(false)
	checker.setHasChecked(true)
	return false
}
