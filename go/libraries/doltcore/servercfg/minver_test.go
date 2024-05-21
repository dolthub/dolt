// Copyright 2024 Dolthub, Inc.
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

package servercfg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/minver"
	"github.com/dolthub/dolt/go/libraries/utils/structwalk"
)

func TestMinVer(t *testing.T) {
	err := structwalk.Walk(&YAMLConfig{}, minver.ValidateMinVerFunc)

	// All new fields must:
	//  1. Have a yaml tag with a name and omitempty
	//  2. They must be nullable
	//  3. They must have a minver tag with a value of TBD which will be replaced with the current version
	//     as part of the release process
	//
	// example:
	//   FieldName *string `yaml:"field_name,omitempty" minver:"TBD"`
	require.NoError(t, err)
}

func TestMinVersionsValid(t *testing.T) {
	minver.ValidateAgainstFile(t, "testdata/minver_validation.txt", &YAMLConfig{})
}
