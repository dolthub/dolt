package pantoerr

import "github.com/attic-labs/noms/go/d"

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
