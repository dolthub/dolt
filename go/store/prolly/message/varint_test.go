// Copyright 2022 Dolthub, Inc.
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

package message

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testRand = rand.New(rand.NewSource(1))

func TestRoundTripVarints(t *testing.T) {
	for k := 0; k < 100; k++ {
		n := testRand.Intn(45) + 5

		counts := make([]uint64, n)
		sum := uint64(0)
		for i := range counts {
			c := testRand.Uint64() % math.MaxUint32
			counts[i] = c
			sum += c
		}
		assert.Equal(t, sum, sumSubtrees(counts))

		// round trip the array
		buf := make([]byte, maxEncodedSize(len(counts)))
		buf = encodeVarints(counts, buf)
		actual := decodeVarints(buf, make([]uint64, n))

		assert.Equal(t, counts, actual)
	}
}
