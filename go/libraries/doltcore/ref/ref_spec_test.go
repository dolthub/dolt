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

package ref

import "testing"

func TestRefSpec(t *testing.T) {
	tests := []struct {
		remote     string
		refSpecStr string
		isValid    bool
		inToExpOut map[string]string
	}{
		{
			"origin",
			"refs/heads/*:refs/remotes/origin/*",
			true,
			map[string]string{
				"refs/heads/main":          "refs/remotes/origin/main",
				"refs/heads/feature":       "refs/remotes/origin/feature",
				"refs/remotes/origin/main": "refs/nil/",
			},
		}, {
			"borigin",
			"refs/heads/main:refs/remotes/borigin/mymain",
			true,
			map[string]string{
				"refs/heads/main":    "refs/remotes/borigin/mymain",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			"",
			"refs/heads/*/main:refs/remotes/borigin/*/mymain",
			true,
			map[string]string{
				"refs/heads/main":    "refs/nil/",
				"refs/heads/bh/main": "refs/remotes/borigin/bh/mymain",
				"refs/heads/as/main": "refs/remotes/borigin/as/mymain",
			},
		}, {
			"",
			"main",
			true,
			map[string]string{
				"refs/heads/main":    "refs/heads/main",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			"",
			"main:main",
			true,
			map[string]string{
				"refs/heads/main":    "refs/heads/main",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			"origin",
			"refs/heads/main:refs/remotes/not_borigin/mymain",
			false,
			nil,
		}, {
			"origin",
			"refs/heads/*:refs/remotes/origin/branchname",
			false,
			nil,
		}, {
			"origin",
			"refs/heads/branchname:refs/remotes/origin/*",
			false,
			nil,
		}, {
			"origin",
			"refs/heads/*/*:refs/remotes/origin/*/*",
			false,
			nil,
		},
	}

	for _, test := range tests {
		var refSpec RefSpec
		var err error

		if test.remote == "" {
			refSpec, err = ParseRefSpec(test.refSpecStr)
		} else {
			refSpec, err = ParseRefSpecForRemote(test.remote, test.refSpecStr)
		}

		if (err == nil) != test.isValid {
			t.Error(test.refSpecStr, "is valid:", err == nil)
		} else if err == nil {
			for in, out := range test.inToExpOut {
				inRef, _ := Parse(in)
				outRef, _ := Parse(out)

				actual := refSpec.DestRef(inRef)

				if !Equals(actual, outRef) {
					t.Error(test.refSpecStr, "mapped", in, "to", actual.String(), "expected", outRef.String())
				}
			}
		}
	}
}
