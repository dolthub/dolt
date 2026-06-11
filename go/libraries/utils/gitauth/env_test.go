// Copyright 2026 Dolthub, Inc.
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

package gitauth

import (
	"slices"
	"testing"
)

// sortedOverrides is the tail NonInteractiveEnv appends to every result.
var sortedOverrides = []string{
	"GCM_INTERACTIVE=Never",
	"GIT_ASKPASS=",
	"GIT_TERMINAL_PROMPT=0",
	"SSH_ASKPASS=",
	"SSH_ASKPASS_REQUIRE=never",
}

func TestNonInteractiveEnv(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11027.
	cases := []struct {
		name string
		env  []string
		want []string
	}{
		{
			name: "nil env yields only overrides",
			env:  nil,
			want: sortedOverrides,
		},
		{
			name: "unrelated entries preserved in order",
			env:  []string{"PATH=/usr/bin", "HOME=/home/u"},
			want: append([]string{"PATH=/usr/bin", "HOME=/home/u"}, sortedOverrides...),
		},
		{
			name: "conflicting keys removed not shadowed",
			env:  []string{"SSH_ASKPASS=/usr/bin/x11-askpass", "GIT_TERMINAL_PROMPT=1", "PATH=/usr/bin"},
			want: append([]string{"PATH=/usr/bin"}, sortedOverrides...),
		},
		{
			name: "bare key without equals matching an override is dropped",
			env:  []string{"SSH_ASKPASS", "PATH=/usr/bin"},
			want: append([]string{"PATH=/usr/bin"}, sortedOverrides...),
		},
		{
			// Windows stores per drive working directories in env entries
			// whose names begin with an equals sign. Stripping them would
			// break the child process so they must pass through.
			name: "empty entry and leading equals entry are kept",
			env:  []string{"", "=C:=C:\\Windows"},
			want: append([]string{"", "=C:=C:\\Windows"}, sortedOverrides...),
		},
		{
			name: "value containing equals does not confuse key match",
			env:  []string{"GIT_SSH_COMMAND=ssh -o ProxyCommand=nc %h %p"},
			want: append([]string{"GIT_SSH_COMMAND=ssh -o ProxyCommand=nc %h %p"}, sortedOverrides...),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(tt.env)
			got := NonInteractiveEnv(tt.env)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("NonInteractiveEnv(%q) = %q, want %q", tt.env, got, tt.want)
			}
			if !slices.Equal(tt.env, in) {
				t.Fatalf("input mutated: %q, want %q", tt.env, in)
			}
		})
	}
}
