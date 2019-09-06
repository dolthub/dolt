package filesys

import (
	"github.com/juju/fslock"
)

const defaultValue int32 = 0

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
	value int32
}

func NewInMemFileLock(fs Filesys) *InMemFileLock {
	return &InMemFileLock{defaultValue}
}

func (memLock *InMemFileLock) TryLock() (bool, error) {
	// do atomic swap
	return true, nil
}

func (memLock *InMemFileLock) Unlock() error {
	// do atomic swap
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
