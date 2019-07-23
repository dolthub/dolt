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
