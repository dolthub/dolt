package cli

import (
	"fmt"
	"os"
	"strings"
)

type MultiError struct {
	Errors []error
}

func NewMultiError(err ...error) MultiError {
	return MultiError{Errors: err}
}

func (m MultiError) Error() string {
	errs := make([]string, len(m.Errors))
	for i, err := range m.Errors {
		errs[i] = err.Error()
	}

	return strings.Join(errs, "\n")
}

// ExitCoder is the interface checked by `App` and `Command` for a custom exit
// code
type ExitCoder interface {
	ExitCode() int
}

// ExitError fulfills both the builtin `error` interface and `ExitCoder`
type ExitError struct {
	exitCode int
	message  string
}

// NewExitError makes a new *ExitError
func NewExitError(message string, exitCode int) *ExitError {
	return &ExitError{
		exitCode: exitCode,
		message:  message,
	}
}

// Error returns the string message, fulfilling the interface required by
// `error`
func (ee *ExitError) Error() string {
	return ee.message
}

// ExitCode returns the exit code, fulfilling the interface required by
// `ExitCoder`
func (ee *ExitError) ExitCode() int {
	return ee.exitCode
}

// HandleExitCoder checks if the error fulfills the ExitCoder interface, and if
// so prints the error to stderr (if it is non-empty) and calls os.Exit with the
// given exit code.  If the given error is a MultiError, then this func is
// called on all members of the Errors slice.
func HandleExitCoder(err error) {
	if exitErr, ok := err.(ExitCoder); ok {
		if err.Error() != "" {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(exitErr.ExitCode())
	}

	if multiErr, ok := err.(MultiError); ok {
		for _, merr := range multiErr.Errors {
			HandleExitCoder(merr)
		}
	}
}
