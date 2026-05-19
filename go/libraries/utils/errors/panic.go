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

package errors

import (
	stderrors "errors"
	"fmt"
	"runtime/debug"
	"syscall"
	"time"
)

type FatalBehavior int

const (
	// Returns an error on a fatal error.
	FatalBehaviorError FatalBehavior = iota

	// Crashes the process immediately and without returning on a fatal error.
	FatalBehaviorCrash
)

// Fatalf signals a fatal error, and can be used in situations where the process may
// be entering an unsafe state due to the encountered error. If |behavior| is
// FatalBehaviorCrash, this function will never return. Otherwise, an error value is
// returned, built with fmt.Errorf on |msg| and |args|.
//
// As a special case, even when |behavior| is FatalBehaviorCrash, if any of |args|
// is an error whose chain contains syscall.ENOSPC, Fatalf returns the error
// instead of crashing the process. Disk-exhaustion is operator-recoverable by
// freeing space; crashing under ENOSPC and being auto-restarted by a supervisor
// triggers another write attempt against the still-full filesystem, which
// fails identically and can leave additional partial files behind. Surfacing
// the error to the caller lets the failure be reported cleanly without the
// recovery cycle amplifying it. See https://github.com/dolthub/dolt/issues/11068.
func Fatalf(behavior FatalBehavior, msg string, args ...any) error {
	if behavior == FatalBehaviorCrash {
		if anyArgIsEnospc(args) {
			return fmt.Errorf("fatal error: "+msg, args...)
		}
		stack := string(debug.Stack())
		args := append([]any{stack}, args...)
		go func() {
			panic(fmt.Errorf("%s\n\nfatal error: "+msg, args...).Error())
		}()
		for {
			time.Sleep(60 * time.Minute)
		}
	} else {
		return fmt.Errorf("fatal error: "+msg, args...)
	}
}

// anyArgIsEnospc reports whether any of |args| is an error whose unwrap chain
// contains syscall.ENOSPC. Used by Fatalf to distinguish disk-exhaustion from
// other fatal conditions.
func anyArgIsEnospc(args []any) bool {
	for _, a := range args {
		if e, ok := a.(error); ok && stderrors.Is(e, syscall.ENOSPC) {
			return true
		}
	}
	return false
}
