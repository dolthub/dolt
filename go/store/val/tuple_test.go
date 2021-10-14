// Copyright 2021 Dolthub, Inc.
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

package val

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tupPool buffPool

func TestNewTuple(t *testing.T) {
	t.Run("test tuple round trip", func(t *testing.T) {
		testRoundTrip(t)
	})
}

func testRoundTrip(t *testing.T) {
	for n := 0; n < 100; n++ {
		vals := randomValues(t)
		tup := NewTuple(tupPool, vals...)
		for i, v := range vals {
			assert.Equal(t, v.Val, tup.GetField(i))
		}
	}
}

func randomValues(t *testing.T) (vals []Value) {
	vals = make([]Value, (rand.Uint32() % 19)+1)
	assert.True(t, len(vals)>0)

	for i := range vals {
		vals[i] = Value{
			Typ: BinaryType,
			Val: nil,
		}

		if rand.Uint32()%4 == 0 {
			// 25% NULL
			continue
		}

		vals[i].Val = make([]byte, rand.Uint32()%20)
		rand.Read(vals[i].Val)
	}
	return
}