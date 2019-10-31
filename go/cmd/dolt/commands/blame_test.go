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

package commands

import "testing"

func TestTruncateString(t *testing.T) {
	type args struct {
		str       string
		maxLength int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"negative maxLength", args{"foo", -1}, "foo"},
		{"maxLength 0", args{"foo", 0}, ""},
		{"maxLength 1", args{"foo", 1}, "…"},
		{"maxLength less than word length", args{"foo", 2}, "f…"},
		{"maxLength equal to word length", args{"foo", 3}, "foo"},
		{"maxLength greater than word length", args{"foo", 4}, "foo"},
		{"empty string with maxLength 0", args{"", 0}, ""},
		{"empty string with maxLength 1", args{"", 1}, ""},
		{"one-letter string with maxLength 1", args{"a", 1}, "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncateString(tt.args.str, tt.args.maxLength); got != tt.want {
				t.Errorf("truncateString() = %v, want %v", got, tt.want)
			}
		})
	}
}
