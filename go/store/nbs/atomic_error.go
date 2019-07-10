package nbs

import (
	"sync"
	"sync/atomic"
)

type AtomicError struct {
	once *sync.Once
	val  *atomic.Value
}

func NewAtomicError() *AtomicError {
	return &AtomicError{&sync.Once{}, &atomic.Value{}}
}

func (ae *AtomicError) SetIfError(err error) {
	if err != nil {
		ae.once.Do(func() {
			ae.val.Store(err)
		})
	}
}

func (ae *AtomicError) IsSet() bool {
	val := ae.val.Load()
	return val != nil
}

func (ae *AtomicError) Get() error {
	val := ae.val.Load()

	if val == nil {
		return nil
	}

	return val.(error)
}

func (ae *AtomicError) Error() string {
	err := ae.Get()

	if err != nil {
		return err.Error()
	}

	return ""
}
