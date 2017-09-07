// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

var pattern = regexp.MustCompile("([0-9a-v]{" + strconv.Itoa(hash.StringLen) + "})")

// EqualsIgnoreHashes compares two strings, ignoring hashes in them.
func EqualsIgnoreHashes(tt *testing.T, expected, actual string) {
	if RemoveHashes(expected) != RemoveHashes(actual) {
		assert.Equal(tt, expected, actual)
	}
}

func RemoveHashes(str string) string {
	return pattern.ReplaceAllString(str, strings.Repeat("*", hash.StringLen))
}
