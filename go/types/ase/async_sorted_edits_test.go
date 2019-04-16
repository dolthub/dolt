package ase

import (
	"github.com/attic-labs/noms/go/types"
	"math/rand"
	"testing"
	"time"
)

func createKVPs(rng *rand.Rand, size int) KVPSlice {
	kvps := make(KVPSlice, size)

	for i := 0; i < size; i++ {
		kvps[i] = KVP{types.Uint(rng.Uint64() % 10000), types.NullValue}
	}

	return kvps
}

func TestAsyncSortedEdits(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	testASE(t, rng)

	for i := 0; i < 128; i++ {
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
	asyncSorted := NewAsyncSortedEdits(buffSize, asyncSortConcurrency, sortConcurrency)

	for _, kvp := range kvps {
		asyncSorted.Set(kvp.Key, kvp.Val)
	}

	asyncSorted.FinishedEditing()

	expectedKVPColls := (numKVPs + (buffSize - 1)) / buffSize
	actualKVPCols := len(asyncSorted.sortedColls)

	if expectedKVPColls != actualKVPCols {
		t.Error("unexpected buffer count. expected:", expectedKVPColls, "actual count:", actualKVPCols)
	}

	asyncSorted.Sort()

	if asyncSorted.Size() != int64(numKVPs) {
		t.Error("Invalid count", asyncSorted.Size(), "!=", numKVPs)
	}

	itr := asyncSorted.Iterator()
	inOrder, count := IsInOrder(itr)

	if count != numKVPs {
		t.Error("Invalid count", count, "!=", numKVPs)
	}

	if !inOrder {
		t.Error("Not in order")
	}
}
