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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
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
