// Copyright 2019 Liquidata, Inc.
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

package filesys

import (
	"sync/atomic"

	"github.com/juju/fslock"
	"github.com/pkg/errors"
)

const unlockedStateValue int32 = 0
const lockedStateValue int32 = 1

// errLockUnlock occurs if there is an error unlocking the lock
var errLockUnlock = errors.New("unable to unlock the lock")

// FilesysLock is an interface for locking and unlocking filesystems
type FilesysLock interface {
	TryLock() (bool, error)
	Unlock() error
}

// CreateFilesysLock creates a new FilesysLock
func CreateFilesysLock(fs Filesys, filename string) FilesysLock {
	switch fs.(type) {
	case *InMemFS:
		return NewInMemFileLock(fs)
	case *localFS:
		return NewLocalFileLock(fs, filename)
	default:
		panic("Unsupported file system")
	}
}

// InMemFileLock is a lock for the InMemFS
type InMemFileLock struct {
	state int32
}

// NewInMemFileLock creates a new InMemFileLock
func NewInMemFileLock(fs Filesys) *InMemFileLock {
	return &InMemFileLock{unlockedStateValue}
}

// TryLock attempts to lock the lock or fails if it is already locked
func (memLock *InMemFileLock) TryLock() (bool, error) {
	if atomic.CompareAndSwapInt32(&memLock.state, unlockedStateValue, lockedStateValue) {
		return true, nil
	}
	return false, nil
}

// Unlock unlocks the lock
func (memLock *InMemFileLock) Unlock() error {
	if memLock.state == 0 {
		return nil
	}

	new := atomic.AddInt32(&memLock.state, -lockedStateValue)

	if new != 0 {
		return errLockUnlock
	}

	return nil
}

// LocalFileLock is the lock for the localFS
type LocalFileLock struct {
	lck *fslock.Lock
}

// NewLocalFileLock creates a new LocalFileLock
func NewLocalFileLock(fs Filesys, filename string) *LocalFileLock {
	lck := fslock.New(filename)

	return &LocalFileLock{lck: lck}
}

// TryLock attempts to lock the lock or fails if it is already locked
func (locLock *LocalFileLock) TryLock() (bool, error) {
	err := locLock.lck.TryLock()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Unlock unlocks the lock
func (locLock *LocalFileLock) Unlock() error {
	err := locLock.lck.Unlock()
	if err != nil {
		return err
	}
	return nil
}
