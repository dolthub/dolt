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

	"github.com/dolthub/dolt/go/store/pool"
)

var testPool = pool.NewBuffPool()

func TestNewTuple(t *testing.T) {
	t.Run("test tuple round trip", func(t *testing.T) {
		roundTripBytes(t)
	})
}

func roundTripBytes(t *testing.T) {
	randomBytes := func(t *testing.T) (fields [][]byte) {
		fields = make([][]byte, (rand.Uint32()%19)+1)
		assert.True(t, len(fields) > 0)
		for i := range fields {
			if rand.Uint32()%4 == 0 {
				// 25% NULL
				continue
			}
			fields[i] = make([]byte, rand.Uint32()%20)
			rand.Read(fields[i])
		}
		return
	}

	for n := 0; n < 100; n++ {
		fields := randomBytes(t)
		tup := NewTuple(testPool, fields...)
		for i, field := range fields {
			assert.Equal(t, field, tup.GetField(i))
		}
	}
}
