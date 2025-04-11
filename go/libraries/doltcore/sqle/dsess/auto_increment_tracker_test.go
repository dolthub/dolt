// Copyright 2023 Dolthub, Inc.
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

package dsess

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoerceAutoIncrementValue(t *testing.T) {
	tests := []struct {
		val interface{}
		exp uint64
		err bool
	}{
		{
			val: nil,
			exp: uint64(0),
		},
		{
			val: int32(0),
			exp: uint64(0),
		},
		{
			val: int32(1),
			exp: uint64(1),
		},
		{
			val: uint32(1),
			exp: uint64(1),
		},
		{
			val: float32(1),
			exp: uint64(1),
		},
		{
			val: float32(1.1),
			exp: uint64(1),
		},
		{
			val: float32(1.9),
			exp: uint64(2),
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("Coerce %v to %v", test.val, test.exp)
		t.Run(name, func(t *testing.T) {
			act, err := CoerceAutoIncrementValue(test.val)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.exp, act)
		})
	}
}
