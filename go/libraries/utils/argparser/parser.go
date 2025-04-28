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
	"fmt"
	"sort"
	"strings"
)

const (
	optNameValDelimChars = " =:"
	whitespaceChars      = " \r\n\t"

	helpFlag       = "help"
	helpFlagAbbrev = "h"
)

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
	Name                 string
	MaxArgs              int
	TooManyArgsErrorFunc func(receivedArgs []string) error
	Supported            []*Option
	nameOrAbbrevToOpt    map[string]*Option
	ArgListHelp          [][2]string
}

// NewArgParserWithMaxArgs creates a new ArgParser for a named command that limits how many positional arguments it
// will accept. If additional arguments are provided, parsing will return an error with a detailed error message,
// using the provided command name.
func NewArgParserWithMaxArgs(name string, maxArgs int) *ArgParser {
	tooManyArgsErrorGenerator := func(receivedArgs []string) error {
		args := strings.Join(receivedArgs, ", ")
		if maxArgs == 0 {
			return fmt.Errorf("error: %s does not take positional arguments, but found %d: %s", name, len(receivedArgs), args)
		}
		return fmt.Errorf("error: %s has too many positional arguments. Expected at most %d, found %d: %s", name, maxArgs, len(receivedArgs), args)
	}
	var supported []*Option
	nameOrAbbrevToOpt := make(map[string]*Option)
	return &ArgParser{
		Name:                 name,
		MaxArgs:              maxArgs,
		TooManyArgsErrorFunc: tooManyArgsErrorGenerator,
		Supported:            supported,
		nameOrAbbrevToOpt:    nameOrAbbrevToOpt,
	}
}

// NewArgParserWithVariableArgs creates a new ArgParser for a named command
// that accepts any number of positional arguments.
func NewArgParserWithVariableArgs(name string) *ArgParser {
	return NewArgParserWithMaxArgs(name, -1)
}

// SupportOption adds support for a new argument with the option given. Options must have a unique name and abbreviated name.
func (ap *ArgParser) SupportOption(opt *Option) {
	name := opt.Name
	abbrev := opt.Abbrev

	_, nameExist := ap.nameOrAbbrevToOpt[name]
	_, abbrevExist := ap.nameOrAbbrevToOpt[abbrev]

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
	ap.nameOrAbbrevToOpt[name] = opt

	if abbrev != "" {
		ap.nameOrAbbrevToOpt[abbrev] = opt
	}
}

// SupportsFlag adds support for a new flag (argument with no value). See SupportOpt for details on params.
func (ap *ArgParser) SupportsFlag(name, abbrev, desc string) *ArgParser {
	opt := &Option{name, abbrev, "", OptionalFlag, desc, nil, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsAlias adds support for an alias for an existing option. The alias can be used in place of the original option.
func (ap *ArgParser) SupportsAlias(alias, original string) *ArgParser {
	opt, ok := ap.nameOrAbbrevToOpt[original]

	if !ok {
		panic(fmt.Sprintf("No option found for %s, this is a bug", original))
	}

	ap.nameOrAbbrevToOpt[alias] = opt
	return ap
}

// SupportsString adds support for a new string argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsString(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, nil, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsRequiredString adds support for a new required string argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsRequiredString(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, RequiredValue, desc, nil, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsStringList adds support for a new string list argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsStringList(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, nil, true}
	ap.SupportOption(opt)

	return ap
}

// SupportsOptionalString adds support for a new string argument with the description given and optional empty value.
func (ap *ArgParser) SupportsOptionalString(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalEmptyValue, desc, nil, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsValidatedString adds support for a new string argument with the description given and defined validation function.
func (ap *ArgParser) SupportsValidatedString(name, abbrev, valDesc, desc string, validator ValidationFunc) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, validator, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsUint adds support for a new uint argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsUint(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isUintStr, false}
	ap.SupportOption(opt)

	return ap
}

// SupportsInt adds support for a new int argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsInt(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isIntStr, false}
	ap.SupportOption(opt)

	return ap
}

// modal options in order of descending string length
func (ap *ArgParser) sortedModalOptions() []string {
	smo := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.nameOrAbbrevToOpt {
		if opt.OptType == OptionalFlag && s != "" {
			smo = append(smo, s)
		}
	}
	sort.Slice(smo, func(i, j int) bool { return len(smo[i]) > len(smo[j]) })
	return smo
}

func (ap *ArgParser) matchModalOptions(arg string) (matches []*Option, rest string) {
	rest = arg

	// try to match longest options first
	candidateFlagNames := ap.sortedModalOptions()

	kontinue := true
	for kontinue {
		kontinue = false

		// stop if we see a value option
		for _, vo := range ap.sortedValueOptions() {
			if rest == vo {
				return matches, rest
			}
			if strings.HasPrefix(rest, vo+"=") {
				return matches, rest
			}
		}

		for i, on := range candidateFlagNames {
			lo := len(on)
			isMatch := len(rest) >= lo && rest[:lo] == on
			if isMatch {
				rest = rest[lo:]
				m := ap.nameOrAbbrevToOpt[on]
				matches = append(matches, m)

				// only match options once
				head := candidateFlagNames[:i]
				var tail []string
				if i+1 < len(candidateFlagNames) {
					tail = candidateFlagNames[i+1:]
				}
				candidateFlagNames = append(head, tail...)

				kontinue = true
				break
			}
		}
	}
	return matches, rest
}

func (ap *ArgParser) sortedValueOptions() []string {
	vos := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.nameOrAbbrevToOpt {
		if (opt.OptType == OptionalValue || opt.OptType == OptionalEmptyValue || opt.OptType == RequiredValue) && s != "" {
			vos = append(vos, s)
		}
	}
	sort.Slice(vos, func(i, j int) bool { return len(vos[i]) > len(vos[j]) })
	return vos
}

func (ap *ArgParser) matchValueOption(arg string, isLongFormFlag bool) (match *Option, value *string) {
	for _, on := range ap.sortedValueOptions() {
		lo := len(on)
		isMatch := len(arg) >= lo && arg[:lo] == on
		if isMatch {
			v := arg[lo:]
			if len(v) > 0 && !strings.Contains(optNameValDelimChars, v[:1]) { // checks if the value and the param is in the same string
				// we only allow joint param and value for short form flags (ie "-" flags), similar to Git's behavior
				if isLongFormFlag {
					return nil, nil
				}
			}

			v = strings.TrimLeft(v, optNameValDelimChars)
			if len(v) > 0 {
				value = &v
			}
			match = ap.nameOrAbbrevToOpt[on]
			return match, value
		}
	}
	return nil, nil
}

func (ap *ArgParser) ParseGlobalArgs(args []string) (apr *ArgParseResults, remaining []string, err error) {
	list := make([]string, 0, 16)
	results := make(map[string]string)

	i := 0
	for ; i < len(args); i++ {
		arg := args[i]

		if len(arg) == 0 || arg == "--" {
			continue
		}

		if arg[0] != '-' {
			// This isn't a flag; assume it's the subcommand. Don't parse the remaining args.
			return &ArgParseResults{results, nil, ap, NO_POSITIONAL_ARGS}, args[i:], nil
		}

		var err error
		i, list, results, err = ap.parseToken(args, i, list, results)

		if err != nil {
			return nil, nil, err
		}
	}

	return nil, nil, errors.New("No valid dolt subcommand found. See 'dolt --help' for usage.")
}

// Parse parses the string args given using the configuration previously specified with calls to the various Supports*
// methods. Any unrecognized arguments or incorrect types will result in an appropriate error being returned. If the
// universal --help or -h flag is found, an ErrHelp error is returned.
func (ap *ArgParser) Parse(args []string) (*ArgParseResults, error) {
	positionalArgs := make([]string, 0, 16)
	positionalArgsSeparatorIndex := NO_POSITIONAL_ARGS
	namedArgs := make(map[string]string)
	onlyPositionalArgsLeft := false

	index := 0
	for ; index < len(args); index++ {
		arg := args[index]

		// empty strings should get passed through like other naked words
		if len(arg) == 0 || arg[0] != '-' || onlyPositionalArgsLeft {
			positionalArgs = append(positionalArgs, arg)
			continue
		}

		if arg == "--" {
			onlyPositionalArgsLeft = true
			positionalArgsSeparatorIndex = len(positionalArgs)
			continue
		}

		var err error
		index, positionalArgs, namedArgs, err = ap.parseToken(args, index, positionalArgs, namedArgs)

		if err != nil {
			return nil, err
		}
	}

	if index < len(args) {
		copy(positionalArgs, args[index:])
	}

	if ap.MaxArgs != -1 && len(positionalArgs) > ap.MaxArgs {
		return nil, ap.TooManyArgsErrorFunc(positionalArgs)
	}

	for _, option := range ap.Supported {
		if option.OptType == RequiredValue {
			if _, ok := namedArgs[option.Name]; !ok {
				return nil, fmt.Errorf("option '%s' is required", option.Name)
			}
		}
	}

	return &ArgParseResults{namedArgs, positionalArgs, ap, positionalArgsSeparatorIndex}, nil
}

func (ap *ArgParser) isOptionOrFlag(s string) bool {
	if len(s) == 0 {
		return false
	} else if s[0] != '-' {
		return false
	}
	s = s[1:]

	if len(s) >= 1 && s[0] == '-' {
		s = s[1:]
	}

	_, ok := ap.nameOrAbbrevToOpt[s]
	return ok
}

func (ap *ArgParser) parseToken(args []string, index int, positionalArgs []string, namedArgs map[string]string) (newIndex int, newPositionalArgs []string, newNamedArgs map[string]string, err error) {
	arg := args[index]

	isLongFormFlag := len(arg) >= 2 && arg[:2] == "--"

	arg = strings.TrimLeft(arg, "-")

	if arg == helpFlag || arg == helpFlagAbbrev {
		return 0, nil, nil, ErrHelp
	}

	modalOpts, rest := ap.matchModalOptions(arg)

	for _, opt := range modalOpts {
		if _, exists := namedArgs[opt.Name]; exists {
			return 0, nil, nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
		}

		namedArgs[opt.Name] = ""
	}

	opt, value := ap.matchValueOption(rest, isLongFormFlag)

	if opt == nil {
		if rest == "" {
			return index, positionalArgs, namedArgs, nil
		}

		if len(modalOpts) > 0 {
			// value was attached to modal flag
			// eg: dolt branch -fdmy_branch
			positionalArgs = append(positionalArgs, rest)
			return index, positionalArgs, namedArgs, nil
		}

		return 0, nil, nil, UnknownArgumentParam{name: arg}
	}

	if _, exists := namedArgs[opt.Name]; exists {
		//already provided
		return 0, nil, nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
	}

	if value == nil {
		next := index + 1
		valueStr := ""
		if next >= len(args) {
			if opt.OptType != OptionalEmptyValue {
				return 0, nil, nil, errors.New("error: no value for option `" + opt.Name + "'")
			}
		} else {
			if opt.AllowMultipleOptions {
				list := getListValues(args[next:])
				valueStr = strings.Join(list, ",")
				index += len(list)
			} else {
				nextArg := args[next]
				if opt.OptType == OptionalEmptyValue {
					if !(nextArg == "--" || ap.isOptionOrFlag(nextArg)) {
						valueStr = args[next]
						index = next
					}
				} else {
					valueStr = args[next]
					index = next
				}
			}
			value = &valueStr
		}
	}

	if opt.Validator != nil {
		err := opt.Validator(*value)

		if err != nil {
			return 0, nil, nil, err
		}
	}

	if value == nil {
		value = new(string)
	}

	namedArgs[opt.Name] = *value
	return index, positionalArgs, namedArgs, nil
}

func getListValues(args []string) []string {
	var values []string

	for _, arg := range args {
		// Stop if another option found
		if arg[0] == '-' || arg == "--" {
			return values
		}
		values = append(values, arg)
	}

	return values
}
