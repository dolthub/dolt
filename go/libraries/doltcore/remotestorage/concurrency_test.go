// Copyright 2019 Dolthub, Inc.
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

package remotestorage

import (
	"math/rand"
	"testing"
	"time"
)

func TestBatchItr(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const maxSize = 5000
	const numTests = 64

	ints := make([]int, maxSize)
	for i := 0; i < maxSize; i++ {
		ints[i] = i
	}

	for i := 0; i < numTests; i++ {
		batchSize := rng.Int()%200 + 1
		size := rng.Int()%maxSize + 1
		sl := ints[:size]

		k := 0
		batchItr(size, batchSize, func(start, end int) (stop bool) {
			currSl := sl[start:end]

			for j := 0; j < len(currSl); j++ {
				if currSl[j] != k {
					t.Fatal("failure. batchSize:", batchSize, "size:", size, "start", start, "end", end, "j", j, "k", k, "currSl[j]", currSl[j], "k", k)
				}

				k++
			}

			return false
		})
	}
}
