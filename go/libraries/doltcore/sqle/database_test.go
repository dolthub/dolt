// Copyright 2019-2020 Liquidata, Inc.
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

package sqle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testKeyFunc(t *testing.T, keyFunc func(string) (bool, string), testVal string, expectedIsKey bool, expectedDBName string) {
	isKey, dbName := keyFunc(testVal)
	assert.Equal(t, expectedIsKey, isKey)
	assert.Equal(t, expectedDBName, dbName)
}

func TestIsKeyFuncs(t *testing.T) {
	testKeyFunc(t, IsHeadKey, "", false, "")
	testKeyFunc(t, IsWorkingKey, "", false, "")
	testKeyFunc(t, IsHeadKey, "dolt_head", true, "dolt")
	testKeyFunc(t, IsWorkingKey, "dolt_head", false, "")
	testKeyFunc(t, IsHeadKey, "dolt_working", false, "")
	testKeyFunc(t, IsWorkingKey, "dolt_working", true, "dolt")
}
