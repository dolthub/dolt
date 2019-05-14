package errhand

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
)

type DErrorBuilder struct {
	dispMsg    string
	details    string
	cause      error
	printUsage bool
}

func BuildDError(dispFmt string, args ...interface{}) *DErrorBuilder {
	dispMsg := dispFmt

	if len(args) > 0 {
		dispMsg = fmt.Sprintf(dispFmt, args...)
	}

	return &DErrorBuilder{dispMsg, "", nil, false}
}

func BuildIf(err error, dispFmt string, args ...interface{}) *DErrorBuilder {
	if err == nil {
		return nil
	}

	dispMsg := dispFmt

	if len(args) > 0 {
		dispMsg = fmt.Sprintf(dispFmt, args...)
	}

	return &DErrorBuilder{dispMsg, "", err, false}
}

func (builder *DErrorBuilder) AddDetails(detailsFmt string, args ...interface{}) *DErrorBuilder {
	if builder == nil {
		return nil
	}

	var details string
	if len(args) > 0 {
		details = fmt.Sprintf(detailsFmt, args...)
	} else {
		details = detailsFmt
	}

	if len(builder.details) > 0 {
		builder.details += "\n"
	}

	builder.details += details
	return builder
}

func (builder *DErrorBuilder) AddCause(cause error) *DErrorBuilder {
	if builder == nil {
		return nil
	}

	builder.cause = cause
	return builder
}

func (builder *DErrorBuilder) SetPrintUsage() *DErrorBuilder {
	if builder == nil {
		return nil
	}

	builder.printUsage = true
	return builder
}

func (builder *DErrorBuilder) Build() VerboseError {
	if builder == nil {
		return nil
	}

	return &DError{builder.dispMsg, builder.details, builder.cause, builder.printUsage}
}

type DError struct {
	displayMsg string
	details    string
	cause      error
	printUsage bool
}

// Returns a verbose error using the error given. If the error given is already a VerboseError, returns it. Otherwise,
// creates a new VerboseError with the given error's error string.
func VerboseErrorFromError(err error) VerboseError {
	if err == nil {
		return nil
	}

	if verr, ok := err.(VerboseError); ok {
		return verr
	}

	builder := &DErrorBuilder{err.Error(), "", nil, false}
	return builder.Build()
}

func NewDError(dispMsg, details string, cause error, printUsage bool) *DError {
	return &DError{dispMsg, details, cause, printUsage}
}

func (derr *DError) Error() string {
	return color.RedString(derr.displayMsg)
}

func (derr *DError) Verbose() string {
	sections := make([]string, 0, 6)

	if derr.displayMsg != "" {
		sections = append(sections, derr.Error())
	}

	if derr.details != "" {
		sections = append(sections, derr.details)
	}

	if derr.cause != nil {
		var causeStr string
		if vCause, ok := derr.cause.(VerboseError); ok {
			causeStr = vCause.Verbose()
		} else {
			causeStr = derr.cause.Error()
		}

		indentStr := indent(causeStr, "       ")
		indentStrBytes := []byte(indentStr)
		copy(indentStrBytes, "cause:")

		sections = append(sections, string(indentStrBytes))
	}

	return strings.Join(sections, "\n")
}

func (derr *DError) ShouldPrintUsage() bool {
	return derr.printUsage
}

func indent(str, indentStr string) string {
	lines := strings.Split(str, "\n")
	return indentStr + strings.Join(lines, "\n"+indentStr)
}
