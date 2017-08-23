package multierr

import (
	"fmt"
)

// Error contains a set of errors. Used to return multiple errors, as in listen.
type Error struct {
	Errors []error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil error>"
	}
	var out string
	for i, v := range e.Errors {
		if v != nil {
			out += fmt.Sprintf("%d: %s\n", i, v)
		}
	}
	return out
}

func New(errs ...error) *Error {
	return &Error{errs}
}
