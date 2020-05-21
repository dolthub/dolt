// Copyright 2019 Liquidata, Inc.
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
	"sort"
	"strings"
)

const (
	optNameValDelimChars = " =:"
	whitespaceChars      = " \r\n\t"

	helpFlag = "help"
	helpFlagAbbrev = "h"
)

var helpOption = &Option{ Name: helpFlag, Abbrev: helpFlagAbbrev, OptType: OptionalFlag}

func ValidatorFromStrList(paramName string, validStrList []string) ValidationFunc {
	errSuffix := " is not a valid option for '" + paramName + "'. valid options are: " + strings.Join(validStrList, "|")
	validStrSet := make(map[string]struct{})

	for _, str := range validStrList {
		validStrSet[strings.ToLower(str)] = struct{}{}
	}

	return func(s string) error {
		_, ok := validStrSet[strings.ToLower(s)]

		if !ok {
			return errors.New(s + errSuffix)
		}

		return nil
	}
}

type ArgParser struct {
	Supported         []*Option
	NameOrAbbrevToOpt map[string]*Option
	ArgListHelp       [][2]string
}

func NewArgParser() *ArgParser {
	var supported []*Option
	nameOrAbbrevToOpt := make(map[string]*Option)
	return &ArgParser{supported, nameOrAbbrevToOpt, nil}
}

// Adds support for a new argument with the option given. Options must have a unique name and abbreviated name.
func (ap *ArgParser) SupportOption(opt *Option) {
	name := opt.Name
	abbrev := opt.Abbrev

	_, nameExist := ap.NameOrAbbrevToOpt[name]
	_, abbrevExist := ap.NameOrAbbrevToOpt[abbrev]

	if name == "" {
		panic("Name is required")
	} else if name == "help" || abbrev == "help" || name == "h" || abbrev == "h" {
		panic(`"help" and "h" are both reserved`)
	} else if nameExist || abbrevExist {
		panic("There is a bug.  Two supported arguments have the same name or abbreviation")
	} else if name[0] == '-' || (len(abbrev) > 0 && abbrev[0] == '-') {
		panic("There is a bug. Option names, and abbreviations should not start with -")
	} else if strings.IndexAny(name, optNameValDelimChars) != -1 || strings.IndexAny(name, whitespaceChars) != -1 {
		panic("There is a bug.  Option name contains an invalid character")
	}

	ap.Supported = append(ap.Supported, opt)
	ap.NameOrAbbrevToOpt[name] = opt

	if abbrev != "" {
		ap.NameOrAbbrevToOpt[abbrev] = opt
	}
}

// Adds support for a new flag (argument with no value). See SupportOpt for details on params.
func (ap *ArgParser) SupportsFlag(name, abbrev, desc string) *ArgParser {
	opt := &Option{name, abbrev, "", OptionalFlag, desc, nil}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new string argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsString(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, nil}
	ap.SupportOption(opt)

	return ap
}

func (ap *ArgParser) SupportsValidatedString(name, abbrev, valDesc, desc string, validator ValidationFunc) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, validator}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new uint argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsUint(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isUintStr}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new int argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsInt(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isIntStr}
	ap.SupportOption(opt)

	return ap
}

func oldSplitOption(optStr string) (string, *string) {
	optStr = strings.TrimLeft(optStr, "-")

	idx := strings.IndexAny(optStr, optNameValDelimChars)

	if idx == -1 {
		return strings.TrimSpace(optStr), nil
	}

	argName := strings.TrimSpace(optStr[:idx])
	argValue := strings.TrimSpace(optStr[idx+1:])

	if len(argValue) == 0 {
		return argName, nil
	}

	return argName, &argValue
}

// modal options in order of descending string length
func (ap *ArgParser) sortedModalOptions() []string {
	smo := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.NameOrAbbrevToOpt {
		if opt.OptType == OptionalFlag {
			smo = append(smo, s)
		}
	}
	sort.Slice(smo, func(i, j int) bool { return len(smo[i]) > len(smo[j]) })
	return smo
}

func (ap *ArgParser) matchModalOptions(arg string) (matches []*Option, rest string) {
	rest = arg

	// try to match longest options first
	candidateOptNames := ap.sortedModalOptions()

	kontinue := true
	for kontinue {
		kontinue = false

		for i, on := range candidateOptNames {
			lo := len(on)
			isMatch := len(rest) >= lo && rest[:lo] == on
			if isMatch {
				rest = rest[lo:]
				m := ap.NameOrAbbrevToOpt[on]
				matches = append(matches, m)

				// only match options once
				head := candidateOptNames[:i]
				var tail []string
				if i+1 < len(candidateOptNames) {
					tail = candidateOptNames[i+1:]
				}
				candidateOptNames = append(head, tail...)

				kontinue = true
				break
			}
		}

		isHelp :=  len(rest) >= len(helpFlag) && rest[:len(helpFlag)] == helpFlag ||
			len(rest) >= len(helpFlagAbbrev) && rest[:len(helpFlagAbbrev)] == helpFlagAbbrev
		if isHelp {
			return []*Option{helpOption}, ""
		}
	}
	return matches, rest
}

func (ap *ArgParser) sortedValueOptions() []string {
	vos := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.NameOrAbbrevToOpt {
		if opt.OptType == OptionalValue {
			vos = append(vos, s)
		}
	}
	sort.Slice(vos, func(i, j int) bool { return len(vos[i]) > len(vos[j]) })
	return vos
}

func (ap *ArgParser) matchValueOption(arg string) (match *Option, value *string) {
	for _, on := range ap.sortedValueOptions() {
		lo := len(on)
		isMatch := len(arg) >= lo && arg[:lo] == on
		if isMatch {
			v := arg[lo:]
			v = strings.TrimLeft(v, optNameValDelimChars)
			if len(v) > 0 {
				value = &v
			}
			match = ap.NameOrAbbrevToOpt[on]
			return match, value
		}
	}
	return nil, nil
}

// Parses the string args given using the configuration previously specified with calls to the various Supports*
// methods. Any unrecognized arguments or incorrect types will result in an appropriate error being returned. If the
// universal --help or -h flag is found, an ErrHelp error is returned.
func (ap *ArgParser) Parse(args []string) (*ArgParseResults, error) {
	list := make([]string, 0, 16)
	results := make(map[string]string)

	i := 0
	for ; i < len(args); i++ {
		arg := args[i]

		if len(arg) == 0 || arg[0] != '-' || arg == "--" { // empty strings should get passed through like other naked words
			list = append(list, arg)
			continue
		}

		arg = strings.TrimLeft(arg, "-")

		modalOpts, rest := ap.matchModalOptions(arg)

		for _, opt := range modalOpts {
			if opt == helpOption {
				return nil, ErrHelp
			}

			if _, exists := results[opt.Name]; exists {
				return nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
			}

			results[opt.Name] = ""
		}

		opt, value := ap.matchValueOption(rest)

		if opt == nil {
			if rest == "" {
				continue
			}

			if len(modalOpts) > 0 {
				// value was attached to modal flag
				// eg: dolt branch -fdmy_branch
				list = append(list, rest)
				continue
			}

			return nil, UnknownArgumentParam{name: arg}
		}

		if _, exists := results[opt.Name]; exists {
			//already provided
			return nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
		}

		if value == nil {
			i++
			if i >= len(args) {
				return nil, errors.New("error: no value for option `" + opt.Name + "'")
			}

			valueStr := args[i]
			value = &valueStr
		}

		if opt.Validator != nil {
			err := opt.Validator(*value)

			if err != nil {
				return nil, err
			}
		}

		results[opt.Name] = *value
	}

	if i < len(args) {
		copy(list, args[i:])
	}

	return &ArgParseResults{results, list, ap}, nil
}
