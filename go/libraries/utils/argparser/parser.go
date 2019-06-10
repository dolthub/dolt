package argparser

import (
	"strings"

	"github.com/pkg/errors"
)

const (
	optNameValDelimChars = " =:"
	whitespaceChars      = " \r\n\t"
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
	Supported         []*Option
	NameOrAbbrevToOpt map[string]*Option
	ArgListHelp       map[string]string
}

func NewArgParser() *ArgParser {
	var supported []*Option
	nameOrAbbrevToOpt := make(map[string]*Option)
	argListHelp := make(map[string]string)
	return &ArgParser{supported, nameOrAbbrevToOpt, argListHelp}
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
func (ap *ArgParser) SupportsFlag(name, abbrev, desc string) {
	opt := &Option{name, abbrev, "", OptionalFlag, desc, nil}
	ap.SupportOption(opt)
}

// Adds support for a new string argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsString(name, abbrev, valDesc, desc string) {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, nil}
	ap.SupportOption(opt)
}

func (ap *ArgParser) SupportsValidatedString(name, abbrev, valDesc, desc string, validator ValidationFunc) {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, validator}
	ap.SupportOption(opt)
}

// Adds support for a new uint argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsUint(name, abbrev, valDesc, desc string) {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isUintStr}
	ap.SupportOption(opt)
}

// Adds support for a new int argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsInt(name, abbrev, valDesc, desc string) {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isIntStr}
	ap.SupportOption(opt)
}

func splitOption(optStr string) (string, *string) {
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

// Parses the string args given using the configuration previously specified with calls to the various Supports*
// methods. Any unrecognized arguments or incorrect types will result in an appropriate error being returned. If the
// universal --help or -h flag is found, an ErrHelp error is returned.
func (ap *ArgParser) Parse(args []string) (*ArgParseResults, error) {
	var list []string
	results := make(map[string]string)

	i := 0
	for ; i < len(args); i++ {
		arg := args[i]

		if len(arg) == 0 || arg[0] != '-' { // empty strings should get passed through like other naked words
			list = append(list, arg)
		} else {
			if arg == "--" {
				break
			}

			optName, value := splitOption(arg)

			if optName == "help" || optName == "h" {
				return nil, ErrHelp
			}

			supOpt, ok := ap.NameOrAbbrevToOpt[optName]

			if !ok {
				return nil, UnknownArgumentParam{optName}
			} else {
				if _, exists := results[optName]; exists {
					//already provided
					return nil, errors.New("error: flag `" + supOpt.Name + "' should not have a value")
				}

				if supOpt.OptType == OptionalFlag {
					if value != nil {
						return nil, errors.New("error: multiple values provided for `" + supOpt.Name + "'")
					}

					results[supOpt.Name] = ""
				} else {
					if value == nil {
						i++
						if i >= len(args) {
							return nil, errors.New("error: no value for option `" + arg + "'")
						}

						valueStr := args[i]
						value = &valueStr
					}

					if supOpt.Validator != nil {
						err := supOpt.Validator(*value)

						if err != nil {
							return nil, err
						}
					}

					results[supOpt.Name] = *value
				}
			}
		}
	}

	if i < len(args) {
		copy(list, args[i:])
	}

	return &ArgParseResults{results, list, ap}, nil
}
