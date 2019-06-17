package retry

import (
	"math/rand"
	"time"
)

var retryRand = rand.New(rand.NewSource(time.Now().UnixNano()))

type RetriableCallState int

const (
	RetriableFailure RetriableCallState = iota
	PermanentFailure
	Success
)

type RetryParams struct {
	NumRetries int
	MaxDelay   time.Duration
	Backoff    time.Duration
}

type RetriableCall func() RetriableCallState

func CallWithRetries(rp RetryParams, rc RetriableCall) (success bool) {
	maxDelay := rp.Backoff

	for numRetries := rp.NumRetries; numRetries >= 0; numRetries-- {
		res := rc()

		switch res {
		case Success:
			return true
		case PermanentFailure:
			return false
		case RetriableFailure:
			if numRetries == 0 {
				return false
			}
		}

		delay := time.Duration(float64(maxDelay) * retryRand.Float64())

		time.Sleep(delay)
		maxDelay *= 2

		if maxDelay >= rp.MaxDelay {
			maxDelay = rp.MaxDelay
		}
	}

	// shouldn't actually be reachable
	return false
}
