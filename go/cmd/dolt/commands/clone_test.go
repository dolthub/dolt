// Copyright 2022 Dolthub, Inc.
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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDolthubRepos(t *testing.T) {
	tests := []struct {
		urlStr   string
		expected string
	}{
		{
			urlStr:   "https://www.dolthub.com/repositories/web3/bitcoin-fast",
			expected: "web3/bitcoin-fast",
		},
		{
			urlStr:   "https://www.dolthub.com/repositories/web3/bitcoin-fast/pulls",
			expected: "web3/bitcoin-fast/pulls",
		},
		{
			urlStr:   "https://www.dolthub.com/repositories",
			expected: "",
		},
		{
			urlStr:   "https://www.dolthub.com/repositories/test",
			expected: "test",
		},
		{
			urlStr:   "https://www.notdolthub.com/repositories/dads",
			expected: "",
		},
		{
			urlStr:   "http://www.dolthub.com/repositories",
			expected: "",
		},
		{
			urlStr:   "https://www.dolthub.com/repositories/dolthub/museum-collections",
			expected: "dolthub/museum-collections",
		},
	}

	for _, test := range tests {
		received, ok := validateAndParseDolthubUrl(test.urlStr)
		if test.expected == "" {
			require.False(t, ok)
		} else {
			require.Equal(t, test.expected, received)
		}
	}

}

func TestCloneParseArgs_InferDir(t *testing.T) {
	ap := CloneCmd{}.ArgParser()
	apr, err := ap.Parse([]string{"https://example.com/org/repo.git"})
	require.NoError(t, err)

	dir, urlStr, verr := parseArgs(apr)
	require.Nil(t, verr)
	require.Equal(t, "repo.git", dir)
	require.Equal(t, "https://example.com/org/repo.git", urlStr)
}
