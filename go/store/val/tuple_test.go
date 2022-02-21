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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/pool"
)

var testPool = pool.NewBuffPool()

// todo(andy): randomize test seed
var testRand = rand.New(rand.NewSource(1))

func TestNewTuple(t *testing.T) {
	t.Run("test tuple round trip", func(t *testing.T) {
		roundTripBytes(t)
	})
	t.Run("test tuple get many", func(t *testing.T) {
		testTupleGetMany(t)
	})
}

func roundTripBytes(t *testing.T) {
	for n := 0; n < 100; n++ {
		fields := randomByteFields(t)
		tup := NewTuple(testPool, fields...)
		for i, field := range fields {
			assert.Equal(t, field, tup.GetField(i))
		}
	}
}

func testTupleGetMany(t *testing.T) {
	for n := 0; n < 1000; n++ {
		fields := randomByteFields(t)
		tup := NewTuple(testPool, fields...)

		indexes := randomFieldIndexes(fields)
		actual := tup.GetManyFields(indexes, make([][]byte, len(indexes)))

		for k, idx := range indexes {
			exp := fields[idx]
			act := actual[k]
			assert.Equal(t, exp, act)
		}
	}
}

func randomByteFields(t *testing.T) (fields [][]byte) {
	fields = make([][]byte, rand.Intn(19)+1)
	assert.True(t, len(fields) > 0)
	for i := range fields {
		if rand.Uint32()%4 == 0 {
			// 25% NULL
			fields[i] = nil
			continue
		}
		fields[i] = make([]byte, rand.Intn(19)+1)
		rand.Read(fields[i])
	}
	return
}

func randomFieldIndexes(fields [][]byte) []int {
	indexes := make([]int, len(fields))
	for i := range indexes {
		indexes[i] = i
	}

	k := testRand.Intn(len(indexes))
	if k == 0 {
		k++
	}

	testRand.Shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})
	indexes = indexes[:k]
	sort.Ints(indexes)

	return indexes
}
