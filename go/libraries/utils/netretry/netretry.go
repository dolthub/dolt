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
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

// MaxRetryAfter bounds how long a server's Retry-After header can delay a
// single retry.
const MaxRetryAfter = time.Minute

// WithS3Retryer gives an S3 client this policy: the same attempt count and
// backoff schedule, no client-side retry quota, and HTTP 429 treated as a
// retryable throttle. Proxies in front of S3 can answer a burst of reads with a
// bare 429, which the SDK does not otherwise retry. A client configured for
// adaptive retry mode keeps it.
func WithS3Retryer(o *s3.Options) {
	standard := func(so *retry.StandardOptions) {
		so.MaxAttempts = Count + 1
		so.Backoff = retry.BackoffDelayerFunc(backoffDelay)
		so.RateLimiter = ratelimit.None
		so.Retryables = append(so.Retryables, retry.RetryableHTTPStatusCode{
			Codes: map[int]struct{}{http.StatusTooManyRequests: {}},
		})
	}
	if o.RetryMode == aws.RetryModeAdaptive {
		o.Retryer = retry.NewAdaptiveMode(func(ao *retry.AdaptiveModeOptions) {
			ao.StandardOptions = append(ao.StandardOptions, standard)
			ao.Throttles = append(ao.Throttles, retry.IsErrorThrottleFunc(isTooManyRequests))
		})
		return
	}
	o.Retryer = retry.NewStandard(standard)
}

func isTooManyRequests(err error) aws.Ternary {
	var resp interface{ HTTPStatusCode() int }
	if errors.As(err, &resp) && resp.HTTPStatusCode() == http.StatusTooManyRequests {
		return aws.TrueTernary
	}
	return aws.UnknownTernary
}

// backoffDelay returns the delay before retry |attempt|, counting from 1, as
// NewBackOff would produce it, or the response's Retry-After if that is longer.
func backoffDelay(attempt int, err error) (time.Duration, error) {
	schedule := NewBackOff()
	var delay time.Duration
	for i := 0; i < attempt; i++ {
		delay = schedule.NextBackOff()
	}
	return max(delay, retryAfter(err, time.Now())), nil
}

// retryAfter returns the delay requested by the Retry-After header of the
// response that failed with |err|, bounded by MaxRetryAfter, or 0 if there is
// no valid header.
func retryAfter(err error, now time.Time) time.Duration {
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) || respErr.Response == nil {
		return 0
	}
	header := strings.TrimSpace(respErr.Response.Header.Get("Retry-After"))
	if header == "" {
		return 0
	}
	var delay time.Duration
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil {
		if seconds < 0 {
			return 0
		}
		delay = time.Duration(min(seconds, int64(MaxRetryAfter/time.Second))) * time.Second
	} else if at, err := http.ParseTime(header); err == nil {
		delay = at.Sub(now)
	}
	return min(max(delay, 0), MaxRetryAfter)
}
