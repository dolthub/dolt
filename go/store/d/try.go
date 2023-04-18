// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package d implements several debug, error and assertion functions used throughout Noms.
package d

import (
	"errors"
	"fmt"
)

// Assertion is an interface that provides convenient methods for asserting invariants.
// Methods panic if the invariant isn't met.
type Assertion interface {
	NoError(err error)
	True(b bool)
}

// Chk will panic if an assertion made on it fails
var (
	Chk Assertion = &panicker{}
)

type panicker struct{}

func (s *panicker) NoError(err error) {
	PanicIfError(err)
}

func (s *panicker) True(b bool) {
	PanicIfFalse(b)
}

// Panic creates an error using format and args and wraps it in a
// WrappedError which can be handled using Try() and TryCatch()
func Panic(format string, args ...interface{}) {
	if len(args) == 0 {
		err := errors.New(format)
		panic(err)
	}
	err := fmt.Errorf(format, args...)
	panic(err)
}

// PanicIfError panics if the err given is not nil
func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

// PanicIfTrue panics if the bool given is true
func PanicIfTrue(b bool) {
	if b {
		panic(errors.New("expected true"))
	}
}

// PanicIfFalse panics if the bool given is false
func PanicIfFalse(b bool) {
	if !b {
		panic(errors.New("expected false"))
	}
}
