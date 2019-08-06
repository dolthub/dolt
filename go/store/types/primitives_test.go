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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimitives(t *testing.T) {
	data := []Value{
		Bool(true), Bool(false),
		Float(0), Float(-1),
		Float(-0.1), Float(0.1),
	}

	for i := range data {
		for j := range data {
			if i == j {
				assert.True(t, data[i].Equals(data[j]), "Expected value to equal self at index %d", i)
			} else {
				assert.False(t, data[i].Equals(data[j]), "Expected values at indices %d and %d to not equal", i, j)
			}
		}
	}
}

func TestPrimitivesType(t *testing.T) {
	data := []struct {
		v Value
		k NomsKind
	}{
		{Bool(false), BoolKind},
		{Float(0), FloatKind},
	}

	for _, d := range data {
		assert.True(t, mustType(TypeOf(d.v)).Equals(mustType(MakePrimitiveType(d.k))))
	}
}
