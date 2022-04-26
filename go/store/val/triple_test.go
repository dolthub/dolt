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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTriple(t *testing.T) {
	t.Run("test tuple round trip", func(t *testing.T) {
		roundTripTripleFields(t)
	})
}

func roundTripTripleFields(t *testing.T) {
	for n := 0; n < 100; n++ {
		f1 := randomByteFields(t)
		f2 := randomByteFields(t)
		f3 := randomByteFields(t)

		t1 := NewTuple(testPool, f1...)
		t2 := NewTuple(testPool, f2...)
		t3 := NewTuple(testPool, f3...)

		tri := NewTriple(testPool, t1, t2, t3)

		a, b, c := tri.First(), tri.Second(), tri.Third()
		assert.Equal(t, t1, a)
		assert.Equal(t, t2, b)
		assert.Equal(t, t3, c)

		for i, field := range f1 {
			assert.Equal(t, field, a.GetField(i))
		}
		for i, field := range f2 {
			assert.Equal(t, field, b.GetField(i))
		}
		for i, field := range f3 {
			assert.Equal(t, field, c.GetField(i))
		}
	}
}
