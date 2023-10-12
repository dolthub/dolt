// Copyright 2023 Dolthub, Inc.
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

package lockutil

import (
	"errors"
	"sync"
)

// ErrMutexNotLocked is the error returned by AssertRWMutexIsLocked if the specified mutex was not locked.
var ErrMutexNotLocked = errors.New("mutex is not locked")

// AssertRWMutexIsLocked checks if |mu| is locked (without deadlocking if the mutex is locked) and returns nil
// the mutex is locked. If the mutex is NOT locked, the ErrMutexNotLocked error is returned.
func AssertRWMutexIsLocked(mu *sync.RWMutex) error {
	// TryLock allows us to validate that the mutex is locked (without actually locking it and causing a
	// deadlock) and to return an error if we detect that the mutex is NOT locked.
	if mu.TryLock() {
		// If TryLock returns true, that means it was able to successfully acquire the lock on the mutex, which
		// means the caller did NOT have the lock when they called this function, so we return an error, and we
		// also need to release the lock on the mutex that we grabbed while testing whether the mutex was locked.
		defer mu.Unlock()
		return ErrMutexNotLocked
	}
	return nil
}
