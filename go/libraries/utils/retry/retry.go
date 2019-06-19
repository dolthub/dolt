package retry

import (
	"math/rand"
	"time"
)

// RetriableCallState represents how a function called with retries completed.
type RetriableCallState int

const (
	// RetriableFailure means a function called with retries failed in a way that it should be retried
	RetriableFailure RetriableCallState = iota

	// PermanentFailure means a function called with retries failed in a way that should not be retried
	PermanentFailure

	// Success means a function called with retries succeeded
	Success
)

// RetryParams defines how many retries should be made, and the delay between each retry.
type RetryParams struct {
	NumRetries int
	MaxDelay   time.Duration
	Backoff    time.Duration
}

// RetriableCall is a function called with retries
type RetriableCall func() RetriableCallState

// CallWithRetries calls the supplied RetriableCall, and if it fails in a way that is retriable, then it will make
// subsequent calls to the RetriableCall based on the supplied RetryParams
func CallWithRetries(rp RetryParams, rc RetriableCall) (success bool) {
	retryRand := rand.New(rand.NewSource(time.Now().UnixNano()))
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

		delay := time.Duration(retryRand.Int63() % int64(maxDelay))

		if delay > 0 {
			time.Sleep(delay)
		}

		maxDelay *= 2

		if maxDelay > rp.MaxDelay {
			maxDelay = rp.MaxDelay
		}
	}

	// shouldn't actually be reachable
	return false
}
