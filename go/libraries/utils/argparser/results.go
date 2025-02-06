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
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/set"
)

const (
	NO_POSITIONAL_ARGS = -1
)

type ArgParseResults struct {
	options map[string]string
	Args    []string
	parser  *ArgParser

	PositionalArgsSeparatorIndex int
}

// Equals res and other are only considered equal if the order and contents of their arguments
// are the same.
func (res *ArgParseResults) Equals(other *ArgParseResults) bool {
	if len(res.Args) != len(other.Args) || len(res.options) != len(other.options) {
		return false
	}

	for i, arg := range res.Args {
		if other.Args[i] != arg {
			return false
		}
	}

	for k, v := range res.options {
		if otherVal, ok := other.options[k]; !ok || v != otherVal {
			return false
		}
	}

	return true
}

// NewEmptyResults creates a new ArgParseResults object with no arguments or options. Mostly useful for testing.
func NewEmptyResults() *ArgParseResults {
	return &ArgParseResults{options: make(map[string]string), Args: make([]string, 0)}
}

func (res *ArgParseResults) Contains(name string) bool {
	_, ok := res.options[name]
	return ok
}

func (res *ArgParseResults) ContainsArg(name string) bool {
	for _, val := range res.Args {
		if val == name {
			return true
		}
	}
	return false
}

func (res *ArgParseResults) ContainsAll(names ...string) bool {
	for _, name := range names {
		if _, ok := res.options[name]; !ok {
			return false
		}
	}

	return true
}

func (res *ArgParseResults) ContainsAny(names ...string) bool {
	for _, name := range names {
		if _, ok := res.options[name]; ok {
			return true
		}
	}

	return false
}

func (res *ArgParseResults) ContainsMany(names ...string) []string {
	var contains []string
	for _, name := range names {
		if _, ok := res.options[name]; ok {
			contains = append(contains, name)
		}
	}
	return contains
}

func (res *ArgParseResults) GetValue(name string) (string, bool) {
	val, ok := res.options[name]
	return val, ok
}

func (res *ArgParseResults) GetValueList(name string) ([]string, bool) {
	val, ok := res.options[name]
	return strings.Split(val, ","), ok
}

func (res *ArgParseResults) GetValues(names ...string) map[string]string {
	vals := make(map[string]string)

	for _, name := range names {
		if val, ok := res.options[name]; ok {
			vals[name] = val
		}
	}

	return vals
}

// DropValue removes the value for the given name from the results. A new ArgParseResults object is returned without the
// names value. If the value is not present in the results then the original results object is returned.
func (res *ArgParseResults) DropValue(name string) *ArgParseResults {
	if _, ok := res.options[name]; !ok {
		return res
	}

	newNamedArgs := make(map[string]string, len(res.options)-1)
	for flag, val := range res.options {
		if flag != name {
			newNamedArgs[flag] = val
		}
	}

	return &ArgParseResults{newNamedArgs, res.Args, res.parser, NO_POSITIONAL_ARGS}
}

// SetArgument inserts or replaces an argument. A new ArgParseResults object is returned with the new
// argument added. The parser of the original ArgParseResults is used to verify that the option is supported.
//
// If the option is not supported, this is considered a runtime error, and an error is returned to that effect.
func (res *ArgParseResults) SetArgument(name, val string) (*ArgParseResults, error) {
	newNamedArgs := make(map[string]string, len(res.options)+1)
	for flag, origVal := range res.options {
		newNamedArgs[flag] = origVal
	}

	found := false
	// Verify that the options is supported - using the long name
	for _, opt := range res.parser.Supported {
		if opt.Name == name {
			found = true
			break
		}
	}

	if !found {
		return nil, UnknownArgumentParam{name: name}
	}
	newNamedArgs[name] = val

	return &ArgParseResults{newNamedArgs, res.Args, res.parser, res.PositionalArgsSeparatorIndex}, nil
}

func (res *ArgParseResults) MustGetValue(name string) string {
	val, ok := res.options[name]

	if !ok {
		panic("Value not available.")
	}

	return val
}

func (res *ArgParseResults) GetValueOrDefault(name, defVal string) string {
	val, ok := res.options[name]

	if ok {
		return val
	}

	return defVal
}

func (res *ArgParseResults) GetInt(name string) (int, bool) {
	val, ok := res.options[name]

	if !ok {
		return math.MinInt32, false
	}

	intVal, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return math.MinInt32, false
	}

	return int(intVal), true
}

func (res *ArgParseResults) GetUint(name string) (uint64, bool) {
	val, ok := res.options[name]

	if !ok {
		return math.MaxUint64, false
	}

	uintVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return math.MaxUint64, false
	}

	return uintVal, true
}

func (res *ArgParseResults) GetIntOrDefault(name string, defVal int) int {
	n, ok := res.GetInt(name)

	if ok {
		return n
	}

	return defVal
}

func (res *ArgParseResults) NArg() int {
	return len(res.Args)
}

func (res *ArgParseResults) Arg(idx int) string {
	return res.Args[idx]
}

func (res *ArgParseResults) AnyFlagsEqualTo(val bool) *set.StrSet {
	results := make([]string, 0, len(res.parser.Supported))
	for _, opt := range res.parser.Supported {
		if opt.OptType == OptionalFlag {
			name := opt.Name
			_, ok := res.options[name]

			if ok == val {
				results = append(results, name)
			}
		}
	}

	return set.NewStrSet(results)
}

func (res *ArgParseResults) FlagsEqualTo(names []string, val bool) *set.StrSet {
	results := make([]string, 0, len(res.parser.Supported))
	for _, name := range names {
		opt, ok := res.parser.nameOrAbbrevToOpt[name]
		if ok && opt.OptType == OptionalFlag {
			_, ok := res.options[name]

			if ok == val {
				results = append(results, name)
			}
		}
	}

	return set.NewStrSet(results)
}
