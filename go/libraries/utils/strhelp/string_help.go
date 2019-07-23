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

package strhelp

// NthToken returns the Nth token in s, delimited by delim. There is always at least one token: the zeroth token is the
// input string if delim doesn't occur in s. The second return value will be false if there is no Nth token.
func NthToken(s string, delim rune, n int) (string, bool) {
	if n < 0 {
		panic("invalid arguments.")
	}

	prev := 0
	curr := 0
	for ; curr < len(s); curr++ {
		if s[curr] == uint8(delim) {
			n--

			if n >= 0 {
				prev = curr + 1
			} else {
				break
			}
		}
	}

	if n <= 0 && prev <= curr {
		return s[prev:curr], true
	}

	return "", false
}
