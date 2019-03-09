package argparser

import (
	"github.com/pkg/errors"
	"strconv"
)

type OptionType int

const (
	OptionalFlag OptionType = iota
	OptionalValue
)

type ValidationFunc func(string) error

// Convenience validation function that asserts that an arg is an integer
func isIntStr(str string) error {
	_, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid int.")
	}

	return nil
}

// Convenience validation function that asserts that an arg is an unsigned integer
func isUintStr(str string) error {
	_, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid uint.")
	}

	return nil
}

// An Option encapsulates all the information necessary to represent and parse a command line argument.
type Option struct {
	Name      string // Long name for this Option, specified on the command line with --Name. Required.
	Abbrev    string // Abbreviated name for this Option, specified on the command line with -Abbrev. Optional.
	ValDesc   string // Brief description of the Option.
	OptType   OptionType // The type of this option, either a flag or a value.
	Desc      string // Longer help text for the option.
	Validator ValidationFunc // Function to validate an Option after parsing, returning any error.
}
