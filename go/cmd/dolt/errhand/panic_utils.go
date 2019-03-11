package errhand

import "fmt"

func PanicToVError(errMsg string, f func() VerboseError) VerboseError {
	var err VerboseError

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
