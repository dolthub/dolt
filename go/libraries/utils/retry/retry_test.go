package retry

import (
	"sync"
	"testing"
	"time"
)

func TestCallWithRetries(t *testing.T) {
	const (
		goRoutines      = 1000
		testsPerRoutine = 1
		numTests        = goRoutines * testsPerRoutine
		numRetries      = 4

		// within 10% of ideal
		minPercentOfIdeal = 0.90
		maxPercentOfIdeal = 1.10
	)

	rp := RetryParams{
		NumRetries: numRetries,
		MaxDelay:   1000 * time.Millisecond,
		Backoff:    200 * time.Millisecond,
	}

	idealAvgDelays := [numRetries]time.Duration{
		(200 * time.Millisecond) / 2,
		(400 * time.Millisecond) / 2,
		(800 * time.Millisecond) / 2,
		(1000 * time.Millisecond) / 2, //hit the max
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
