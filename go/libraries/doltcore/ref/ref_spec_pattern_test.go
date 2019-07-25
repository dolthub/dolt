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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringPattern(t *testing.T) {
	sp := strPattern("refs/heads/master")

	captured, matchesMaster := sp.matches("refs/heads/master")
	assert.True(t, matchesMaster, "should match master branch ref")
	assert.True(t, captured == "", "nothing to capture")

	captured, matchesFeature := sp.matches("refs/heads/feature")
	assert.False(t, matchesFeature, "shouldn't match feature branch ref")
	assert.True(t, captured == "", "nothing to capture")
}

func TestWildcardPattern(t *testing.T) {
	type patternTest struct {
		refStr           string
		expectedCaptured string
		expectedMatch    bool
	}

	tests := []struct {
		pattern     string
		patternTest []patternTest
	}{
		{
			"refs/heads/*",
			[]patternTest{
				{"refs/heads/master", "master", true},
				{"refs/heads/feature", "feature", true},
				{"refs/heads/bh/my/feature", "bh/my/feature", true},
			},
		},
		{
			"refs/heads/bh/*",
			[]patternTest{
				{"refs/heads/master", "", false},
				{"refs/heads/bh/my/feature", "my/feature", true},
			},
		},
		{
			"refs/heads/*/master",
			[]patternTest{
				{"refs/heads/master", "", false},
				{"refs/heads/bh/master", "bh", true},
				{"refs/heads/as/master", "as", true},
			},
		},
	}

	for _, test := range tests {
		t.Run("'"+test.pattern+"'", func(t *testing.T) {
			wcp := newWildcardPattern(test.pattern)

			for _, patternTest := range test.patternTest {
				t.Run("'"+patternTest.refStr+"'", func(t *testing.T) {
					captured, matches := wcp.matches(patternTest.refStr)

					assert.Truef(t, captured == patternTest.expectedCaptured, "%s != %s", captured, patternTest.expectedCaptured)
					assert.Truef(t, matches == patternTest.expectedMatch, "%b != %b", matches, patternTest.expectedMatch)
				})
			}
		})
	}
}
