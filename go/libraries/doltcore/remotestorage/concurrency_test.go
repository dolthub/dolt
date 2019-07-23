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

package remotestorage

import (
	"errors"
	"math/rand"
	"sync"
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

func TestConcurrentExec(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const maxConcurrency = 256
	const numWorkItems = 5000
	const numUnbufferedTests = 16
	const numBufferedTests = 16
	const numErrTests = 16

	t.Run("Unbuffered", func(t *testing.T) {
		for i := 0; i < numUnbufferedTests; i++ {
			concurrency := (rng.Int() % (maxConcurrency - 1)) + 1
			concurrentTest(t, concurrency, numWorkItems, -1, make(chan int))
		}
	})

	t.Run("Buffered", func(t *testing.T) {
		for i := 0; i < numBufferedTests; i++ {
			concurrency := (rng.Int() % (maxConcurrency - 1)) + 1
			chanBuffSize := rng.Int() % numWorkItems
			concurrentTest(t, concurrency, numWorkItems, -1, make(chan int, chanBuffSize))
		}
	})

	t.Run("Error", func(t *testing.T) {
		for i := 0; i < numErrTests; i++ {
			concurrency := (rng.Int() % (maxConcurrency - 1)) + 1
			chanBuffSize := rng.Int() % numWorkItems
			firstErrIdx := rng.Int() % numWorkItems
			concurrentTest(t, concurrency, numWorkItems, firstErrIdx, make(chan int, chanBuffSize))
		}
	})

	t.Run("more concurrency than work", func(t *testing.T) {
		concurrentTest(t, maxConcurrency*2, numWorkItems, -1, make(chan int))
	})

	t.Run("no work", func(t *testing.T) {
		concurrentTest(t, maxConcurrency, 0, -1, make(chan int))
	})

}

func concurrentTest(t *testing.T, concurrency, numWorkItems, firstErrIdx int, resultChan chan int) {
	work := make([]func() error, numWorkItems)
	shouldError := firstErrIdx > 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for z := range resultChan {
			count++
			if z != numWorkItems*2 {
				t.Error("bad result value")
			}
		}

		if count != numWorkItems {
			t.Error("Didn't get all the results")
		}
	}()

	for i := 0; i < numWorkItems; i++ {
		x := i
		y := numWorkItems*2 - i
		work[i] = func() error {
			if shouldError && i >= firstErrIdx {
				return errors.New("an error")
			}

			resultChan <- x + y
			return nil
		}
	}

	err := concurrentExec(work, concurrency)

	if err != nil != shouldError {
		t.Error("unexpected error value")
	}
}
