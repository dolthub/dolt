package errhand

import (
	"fmt"
	"github.com/fatih/color"
	"strings"
)

type DErrorBuilder struct {
	dispMsg string
	details string
	cause   error
}

func BuildDError(dispFmt string, args ...interface{}) *DErrorBuilder {
	dispMsg := dispFmt

	if len(args) > 0 {
		dispMsg = fmt.Sprintf(dispFmt, args...)
	}

	return &DErrorBuilder{dispMsg, "", nil}
}

func BuildIf(err error, dispFmt string, args ...interface{}) *DErrorBuilder {
	if err == nil {
		return nil
	}

	dispMsg := dispFmt

	if len(args) > 0 {
		dispMsg = fmt.Sprintf(dispFmt, args...)
	}

	return &DErrorBuilder{dispMsg, "", err}
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

func (builder *DErrorBuilder) Build() VerboseError {
	if builder == nil {
		return nil
	}

	return &DError{builder.dispMsg, builder.details, builder.cause}
}

type DError struct {
	DisplayMsg string
	Details    string
	cause      error
}

func NewDError(dispMsg, details string, cause error) *DError {
	return &DError{dispMsg, details, cause}
}

func (derr *DError) Error() string {
	return color.RedString(derr.DisplayMsg)
}

func (derr *DError) Verbose() string {
	sections := make([]string, 0, 6)
	sections = append(sections, derr.Error())

	if derr.Details != "" {
		sections = append(sections, derr.Details)
	}

	if derr.cause != nil {
		sections = append(sections, "cause:")

		var causeStr string
		if vCause, ok := derr.cause.(VerboseError); ok {
			causeStr = vCause.Verbose()
		} else {
			causeStr = derr.cause.Error()
		}

		sections = append(sections, indent(causeStr, "\t\t"))
	}

	return strings.Join(sections, "\n")
}

func indent(str, indentStr string) string {
	lines := strings.Split(str, "\n")
	return indentStr + strings.Join(lines, "\n"+indentStr)
}
