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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefSpec(t *testing.T) {
	tests := []struct {
		remote     string
		refSpecStr string
		isValid    bool
		inToExpOut map[string]string
		skip       bool
	}{
		{
			remote:     "origin",
			refSpecStr: "refs/heads/*:refs/remotes/origin/*",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/heads/main":          "refs/remotes/origin/main",
				"refs/heads/feature":       "refs/remotes/origin/feature",
				"refs/remotes/origin/main": "refs/nil/",
			},
		}, {
			remote:     "borigin",
			refSpecStr: "refs/heads/main:refs/remotes/borigin/mymain",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/heads/main":    "refs/remotes/borigin/mymain",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			refSpecStr: "refs/heads/*/main:refs/remotes/borigin/*/mymain",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/heads/main":    "refs/nil/",
				"refs/heads/bh/main": "refs/remotes/borigin/bh/mymain",
				"refs/heads/as/main": "refs/remotes/borigin/as/mymain",
			},
		}, {
			refSpecStr: "main",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/heads/main":    "refs/heads/main",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			refSpecStr: "main:main",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/heads/main":    "refs/heads/main",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			remote:     "origin",
			refSpecStr: "refs/heads/main:refs/remotes/not_borigin/mymain",
		}, {
			remote:     "origin",
			refSpecStr: "refs/heads/*:refs/remotes/origin/branchname",
		}, {
			remote:     "origin",
			refSpecStr: "refs/heads/branchname:refs/remotes/origin/*",
		}, {
			remote:     "origin",
			refSpecStr: "refs/heads/*/*:refs/remotes/origin/*/*",
		}, {
			refSpecStr: "refs/tags/*:refs/tags/*",
			isValid:    true,
			inToExpOut: map[string]string{
				"refs/tags/v1": "refs/tags/v1",
			},
			skip: true,
		},
	}

	for _, test := range tests {
		t.Run(test.refSpecStr, func(t *testing.T) {
			if test.skip {
				t.Skip()
			}

			var refSpec RefSpec
			var err error

			if test.remote == "" {
				refSpec, err = ParseRefSpec(test.refSpecStr)
			} else {
				refSpec, err = ParseRefSpecForRemote(test.remote, test.refSpecStr)
			}

			if test.isValid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			for in, out := range test.inToExpOut {
				inRef, err := Parse(in)
				require.NoError(t, err)
				// outRef could be nil because of test construction, which is valid
				expectedOutRef, _ := Parse(out)

				outRef := refSpec.DestRef(inRef)
				assert.Equal(t, expectedOutRef, outRef)
			}
		})
	}
}
