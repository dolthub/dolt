package test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

var pattern = regexp.MustCompile("([0-9a-v]{" + strconv.Itoa(hash.StringLen) + "})")

// EqualsIgnoreHashes compares two strings, ignoring hashes in them.
func EqualsIgnoreHashes(tt *testing.T, expected, actual string) {
	expected2 := pattern.ReplaceAllString(expected, strings.Repeat("*", hash.StringLen))
	actual2 := pattern.ReplaceAllString(actual, strings.Repeat("*", hash.StringLen))
	if expected2 != actual2 {
		assert.Equal(tt, expected, actual)
	}
}
