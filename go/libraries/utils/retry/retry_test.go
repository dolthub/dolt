package retry

import (
	"sync"
	"testing"
	"time"
)

func TestCallWithRetries(t *testing.T) {
	const (
		goRoutines      = 1000
		testsPerRoutine = 2
		numTests        = goRoutines * testsPerRoutine
		numRetries      = 5

		// within 10% of ideal
		minPercentOfIdeal = 0.90
		maxPercentOfIdeal = 1.10
	)

	idealAvgDelays := [numRetries]time.Duration{
		(10 * time.Millisecond) / 2,
		(20 * time.Millisecond) / 2,
		(40 * time.Millisecond) / 2,
		(80 * time.Millisecond) / 2,
		(80 * time.Millisecond) / 2, // should hit the max here
	}

	rp := RetryParams{
		NumRetries: numRetries,
		MaxDelay:   80 * time.Millisecond,
		Backoff:    10 * time.Millisecond,
	}

	wg := &sync.WaitGroup{}
	wg.Add(goRoutines)

	times := make([][numRetries + 1]time.Time, numTests)

	// needed to run this in multiple go routines or this tests takes too long.  turning the delays down too much
	// increases the distance from ideal.
	for i := 0; i < goRoutines; i++ {
		offset := i * testsPerRoutine
		go func() {
			defer wg.Done()

			for j := 0; j < testsPerRoutine; j++ {
				var retryNum int
				var currTimes [numRetries + 1]time.Time
				CallWithRetries(rp, func() RetriableCallState {
					currTimes[retryNum] = time.Now()
					retryNum++

					return RetriableFailure
				})

				times[offset+j] = currTimes
			}
		}()
	}

	wg.Wait()

	var sumDelays [numRetries]time.Duration
	for i := 0; i < numTests; i++ {
		currTimes := times[i]
		for j := 0; j < numRetries; j++ {
			sumDelays[j] += currTimes[j+1].Sub(currTimes[j])
		}
	}

	var avgDelay [numRetries]time.Duration
	var percentOfIdeal [numRetries]float64
	for i := 0; i < numRetries; i++ {
		avgDelay[i] = sumDelays[i] / numTests
		percentOfIdeal[i] = float64(avgDelay[i]) / float64(idealAvgDelays[i])
	}

	t.Log(percentOfIdeal)

	for i := 0; i < numRetries; i++ {
		if percentOfIdeal[i] < minPercentOfIdeal || percentOfIdeal[i] > maxPercentOfIdeal {
			t.Errorf("Retry %d was %f of the ideal.", i, percentOfIdeal[i])
		}
	}
}
