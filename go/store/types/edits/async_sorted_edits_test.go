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

package edits

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func createKVPs(rng *rand.Rand, size int) types.KVPSlice {
	kvps := make(types.KVPSlice, size)

	for i := 0; i < size; i++ {
		kvps[i] = types.KVP{Key: types.Uint(rng.Uint64() % 10000), Val: types.NullValue}
	}

	return kvps
}

func TestAsyncSortedEdits(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	testASE(t, rng)

	for i := 0; i < 16; i++ {
		seed := time.Now().UnixNano()
		t.Log(seed)
		rng := rand.New(rand.NewSource(seed))
		testASE(t, rng)
	}
}

func testASE(t *testing.T, rng *rand.Rand) {
	const (
		minKVPS = 1
		maxKVPS = 100000

		maxBuffSize = 100
		minBuffSize = 10

		maxAsyncSortCon = 16
		minAsyncSortCon = 1

		maxSortCon = 16
		minSortCon = 1
	)

	numKVPs := int(minKVPS + rng.Int31n(maxKVPS-minKVPS))
	buffSize := int(minBuffSize + rng.Int31n(maxBuffSize-minBuffSize))
	asyncSortConcurrency := int(minAsyncSortCon + rng.Int31n(maxAsyncSortCon-minAsyncSortCon))
	sortConcurrency := int(minSortCon + rng.Int31n(maxSortCon-minSortCon))

	kvps := createKVPs(rng, numKVPs)
	asyncSorted := NewAsyncSortedEdits(types.Format_7_18, buffSize, asyncSortConcurrency, sortConcurrency)

	for _, kvp := range kvps {
		asyncSorted.AddEdit(kvp.Key, kvp.Val)
	}

	itr, err := asyncSorted.FinishedEditing()

	assert.NoError(t, err)

	if asyncSorted.Size() != int64(numKVPs) {
		t.Error("Invalid count", asyncSorted.Size(), "!=", numKVPs)
	}

	if itr.NumEdits() != int64(numKVPs) {
		t.Error("Invalid itr count", itr.NumEdits(), "!=", numKVPs)
	}

	inOrder, count, err := IsInOrder(itr)

	assert.NoError(t, err)

	if count != numKVPs {
		t.Error("Invalid count", count, "!=", numKVPs)
	}

	if !inOrder {
		t.Error("Not in order")
	}
}
