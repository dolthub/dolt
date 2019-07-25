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

package ref

import "strings"

type pattern interface {
	matches(string) (string, bool)
}

type strPattern string

func (sp strPattern) matches(s string) (string, bool) {
	return "", s == string(sp)
}

type wcPattern struct {
	prefixStr string
	suffixStr string
}

func newWildcardPattern(s string) wcPattern {
	tokens := strings.Split(s, "*")

	if len(tokens) != 2 {
		panic("invalid localPattern")
	}

	return wcPattern{tokens[0], tokens[1]}
}

func (wp wcPattern) matches(s string) (string, bool) {
	if strings.HasPrefix(s, wp.prefixStr) {
		s = s[len(wp.prefixStr):]
		if strings.HasSuffix(s, wp.suffixStr) {
			return s[:len(s)-len(wp.suffixStr)], true
		}
	}

	return "", false
}
