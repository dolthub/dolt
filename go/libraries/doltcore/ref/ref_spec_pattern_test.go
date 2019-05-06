package ref

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringPattern(t *testing.T) {
	sp := StringPattern("refs/heads/master")

	captured, matchesMaster := sp.Matches("refs/heads/master")
	assert.True(t, matchesMaster, "should match master branch ref")
	assert.True(t, captured == "", "nothing to capture")

	captured, matchesFeature := sp.Matches("refs/heads/feature")
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
			wcp := NewWildcardPattern(test.pattern)

			for _, patternTest := range test.patternTest {
				t.Run("'"+patternTest.refStr+"'", func(t *testing.T) {
					captured, matches := wcp.Matches(patternTest.refStr)

					assert.Truef(t, captured == patternTest.expectedCaptured, "%s != %s", captured, patternTest.expectedCaptured)
					assert.Truef(t, matches == patternTest.expectedMatch, "%b != %b", matches, patternTest.expectedMatch)
				})
			}
		})
	}
}
