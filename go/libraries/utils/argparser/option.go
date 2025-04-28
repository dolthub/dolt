// Copyright 2019 Dolthub, Inc.
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

package argparser

import (
	"errors"
	"strconv"
)

type OptionType int

const (
	OptionalFlag OptionType = iota
	OptionalValue
	OptionalEmptyValue
	RequiredValue
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
	// Long name for this Option, specified on the command line with --Name. Required.
	Name string
	// Abbreviated name for this Option, specified on the command line with -Abbrev. Optional.
	Abbrev string
	// Brief description of the Option.
	ValDesc string
	// The type of this option, either a flag or a value.
	OptType OptionType
	// Longer help text for the option.
	Desc string
	// Function to validate an Option after parsing, returning any error.
	Validator ValidationFunc
	// Allows more than one arg to an Option.
	AllowMultipleOptions bool
}
