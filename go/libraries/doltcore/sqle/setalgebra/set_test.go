// Copyright 2020 Liquidata, Inc.
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

package setalgebra

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestEmptySet(t *testing.T) {
	empty := EmptySet{}
	testVal := mustFiniteSet(NewFiniteSet(types.Format_Default, types.String("test")))

	union, err := empty.Union(testVal)
	assert.NoError(t, err)

	assert.Equal(t, testVal, union)

	intersection, err := empty.Intersect(testVal)
	assert.NoError(t, err)

	assert.Equal(t, empty, intersection)
}

func TestUniversalSet(t *testing.T) {
	universal := UniversalSet{}
	testVal := mustFiniteSet(NewFiniteSet(types.Format_Default, types.String("test")))

	union, err := universal.Union(testVal)
	assert.NoError(t, err)

	assert.Equal(t, universal, union)

	intersection, err := universal.Intersect(testVal)
	assert.NoError(t, err)

	assert.Equal(t, testVal, intersection)
}
