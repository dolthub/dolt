// Copyright 2026 Dolthub, Inc.
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

// Package netretry holds the retry policy for requests to remote storage, shared
// by DoltHub-style remotes and S3-backed remotes.
package netretry

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
)

// Count is how many times a failed request is retried by default.
const Count = 5

// NewBackOff returns the delay schedule between retries.
func NewBackOff() *backoff.ExponentialBackOff {
	ret := backoff.NewExponentialBackOff()
	ret.MaxInterval = 5 * time.Second
	return ret
}

// WithS3Retryer gives an S3 client this policy: the same attempt count and
// backoff schedule, no client-side retry quota, and HTTP 429 treated as
// retryable. Proxies in front of S3 can answer a burst of reads with a bare 429,
// which the SDK does not otherwise retry.
func WithS3Retryer(o *s3.Options) {
	o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
		so.MaxAttempts = Count + 1
		so.Backoff = retry.BackoffDelayerFunc(backoffDelay)
		so.RateLimiter = ratelimit.None
		so.Retryables = append(so.Retryables, retry.RetryableHTTPStatusCode{
			Codes: map[int]struct{}{http.StatusTooManyRequests: {}},
		})
	})
}

// backoffDelay returns the delay before retry |attempt|, counting from 1, as
// NewBackOff would produce it.
func backoffDelay(attempt int, _ error) (time.Duration, error) {
	schedule := NewBackOff()
	var delay time.Duration
	for i := 0; i < attempt; i++ {
		delay = schedule.NextBackOff()
	}
	return delay, nil
}
