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

	"github.com/dolthub/dolt/go/libraries/utils/set"
)

type ArgParseResults struct {
	options map[string]string
	args    []string
	parser  *ArgParser
}

func (res *ArgParseResults) Equals(other *ArgParseResults) bool {
	if len(res.args) != len(other.args) || len(res.options) != len(res.options) {
		return false
	}

	for i, arg := range res.args {
		if other.args[i] != arg {
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

func (res *ArgParseResults) Contains(name string) bool {
	_, ok := res.options[name]
	return ok
}

func (res *ArgParseResults) ContainsArg(name string) bool {
	for _, val := range res.args {
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

func (res *ArgParseResults) GetValues(names ...string) map[string]string {
	vals := make(map[string]string)

	for _, name := range names {
		if val, ok := res.options[name]; ok {
			vals[name] = val
		}
	}

	return vals
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

func (res *ArgParseResults) Options() map[string]string {
	return res.options
}

func (res *ArgParseResults) Args() []string {
	return res.args
}

func (res *ArgParseResults) NArg() int {
	return len(res.args)
}

func (res *ArgParseResults) Arg(idx int) string {
	return res.args[idx]
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
		opt, ok := res.parser.NameOrAbbrevToOpt[name]
		if ok && opt.OptType == OptionalFlag {
			_, ok := res.options[name]

			if ok == val {
				results = append(results, name)
			}
		}
	}

	return set.NewStrSet(results)
}
