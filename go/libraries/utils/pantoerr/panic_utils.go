package pantoerr

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

func PanicToErrorInstance(errInstance error, f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errInstance
		}
	}()

	err = f()
	return err
}

func PanicToError(errMsg string, f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &RecoveredPanic{r, errMsg}
		}
	}()

	err = f()
	return err
}
