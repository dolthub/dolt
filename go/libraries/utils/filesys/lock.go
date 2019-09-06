package filesys

import (
	"sync/atomic"

	"github.com/juju/fslock"
	"github.com/pkg/errors"
)

const originalStateValue int32 = 0
const newStateValue int32 = 1

var errLockUnlock = errors.New("unable to unlock the lock")

type FilesysLock interface {
	TryLock() (bool, error)
	Unlock() error
}

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

type InMemFileLock struct {
	state int32
}

func NewInMemFileLock(fs Filesys) *InMemFileLock {
	return &InMemFileLock{originalStateValue}
}

func (memLock *InMemFileLock) TryLock() (bool, error) {
	if atomic.CompareAndSwapInt32(&memLock.state, originalStateValue, newStateValue) {
		return true, nil
	}
	return false, nil
}

func (memLock *InMemFileLock) Unlock() error {
	// if memLock.state == originalStateValue {
	if memLock.state == 0 {
		return nil
	}

	// old := newStateValue
	new := atomic.AddInt32(&memLock.state, -newStateValue)

	// if atomic.CompareAndSwapInt32(&memLock.state, old, new) {
	// 	return nil
	// }

	if new != 0 {
		return errLockUnlock
	}

	return nil
}

type LocalFileLock struct {
	lck *fslock.Lock
}

func NewLocalFileLock(fs Filesys, filename string) *LocalFileLock {
	lck := fslock.New(filename)
	return &LocalFileLock{lck: lck}
}

func (locLock *LocalFileLock) TryLock() (bool, error) {
	err := locLock.lck.TryLock()
	if err != nil {
		if err == fslock.ErrLocked {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (locLock *LocalFileLock) Unlock() error {
	err := locLock.lck.Unlock()
	if err != nil {
		return err
	}
	return nil
}
