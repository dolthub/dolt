package errhand

import (
	"fmt"
)

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

// GetCause returns the non nil result of a recover call
func (rp *RecoveredPanic) GetCause() interface{} {
	return rp.PanicCause
}

func PanicToErrorInstance(errInstance error, f func() error) (err error) {
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

func PanicToError(errMsg string, f func() error) (err error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = &RecoveredPanic{r, errMsg}
			}
		}()

		err = f()
	}()

	return err
}

func PanicToVError(errMsg string, f func() VerboseError) (err VerboseError) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				bdr := BuildDError(errMsg)

				if recErr, ok := r.(error); ok {
					bdr.AddCause(recErr)
				} else {
					bdr.AddDetails(fmt.Sprint(r))
				}

				err = bdr.Build()
			}
		}()

		err = f()
	}()

	return err
}
