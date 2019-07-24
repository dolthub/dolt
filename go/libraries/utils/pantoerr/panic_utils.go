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

package pantoerr

import "github.com/liquidata-inc/dolt/go/store/d"

// RecoverPanic is used to convert panics to errors.  The attic-labs noms codebase loves to panic.  This is not
// idiomatic for Go code. RecoverPanic wraps the cause of the panic retrieved from the recover call, and implements
// the error interface so it can be treated like a standard error.
type RecoveredPanic struct {
	PanicCause interface{}
	ErrMsg     string
}

// Error returns the error message
func (rp *RecoveredPanic) Error() string {
	return rp.ErrMsg
}

func IsRecoveredPanic(err error) bool {
	_, ok := err.(*RecoveredPanic)

	return ok
}

func GetRecoveredPanicCause(err error) interface{} {
	rp, ok := err.(*RecoveredPanic)

	if !ok {
		panic("Check with IsRecoveredPanic before calling GetRecoveredPanicCause")
	}

	return rp.PanicCause
}

// Runs the function given, recovering from any panics and returning the given error instance instead. The
// function can optionally return an error of its own, which will be returned in the non-panic case.
func PanicToErrorInstance(errInstance error, f func() error) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errInstance
			}
		}()
		err = f()
	}()

	return err
}

// Runs the function given, recovering from any panics and returning a RecoveredPanic error with the errMsg given. The
// function can optionally return an error of its own, which will be returned in the non-panic case.
func PanicToError(errMsg string, f func() error) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if re, ok := r.(d.WrappedError); ok {
					r = d.Unwrap(re)
				}

				err = &RecoveredPanic{r, errMsg}
			}
		}()
		err = f()
	}()

	return err
}

// Same as PanicToError, but for functions that don't return errors except in the case of panic.
func PanicToErrorNil(errMsg string, f func()) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if re, ok := r.(d.WrappedError); ok {
					r = d.Unwrap(re)
				}

				err = &RecoveredPanic{r, errMsg}
			}
		}()
		f()
	}()

	return err
}
