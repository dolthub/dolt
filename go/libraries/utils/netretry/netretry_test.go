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

package netretry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestS3Client returns a client in retry |mode| with WithS3Retryer, pointed
// at |handler|.
func newTestS3Client(t *testing.T, mode aws.RetryMode, handler http.HandlerFunc, optFns ...func(*s3.Options)) *s3.Client {
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
		RetryMode:    mode,
	}, append([]func(*s3.Options){WithS3Retryer}, optFns...)...)
}

// fastRetries caps backoff delays so retries run quickly.
func fastRetries(o *s3.Options) {
	o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, time.Millisecond)
}

func TestS3ClientRetriesBareTooManyRequests(t *testing.T) {
	// Adaptive mode paces the client after throttles, so it gets fewer of them
	// to keep the test quick.
	modes := []struct {
		mode              aws.RetryMode
		throttledAttempts int32
	}{
		{aws.RetryModeStandard, 3},
		{aws.RetryModeAdaptive, 1},
	}
	for _, m := range modes {
		t.Run(string(m.mode), func(t *testing.T) {
			throttledAttempts := m.throttledAttempts
			var attempts atomic.Int32
			client := newTestS3Client(t, m.mode, func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) <= throttledAttempts {
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				w.Header().Set("Content-Range", "bytes 0-4/5")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("hello"))
			}, fastRetries)

			out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String("key"),
				Range:  aws.String("bytes=0-4"),
			})
			require.NoError(t, err)
			defer out.Body.Close()
			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(body))
			assert.Equal(t, int32(throttledAttempts+1), attempts.Load())
		})
	}
}

func TestS3RetryerKeepsAdaptiveMode(t *testing.T) {
	noop := func(w http.ResponseWriter, r *http.Request) {}
	client := newTestS3Client(t, aws.RetryModeStandard, noop)
	assert.IsType(t, &retry.Standard{}, client.Options().Retryer)

	client = newTestS3Client(t, aws.RetryModeAdaptive, noop)
	assert.IsType(t, &retry.AdaptiveMode{}, client.Options().Retryer)
	assert.Equal(t, Count+1, client.Options().Retryer.MaxAttempts())
}

func TestS3RetryerKeepsAdaptiveModeFromEnvironment(t *testing.T) {
	t.Setenv("AWS_RETRY_MODE", "adaptive")
	t.Setenv("AWS_REGION", "us-east-1")
	cfg, err := config.LoadDefaultConfig(context.Background())
	require.NoError(t, err)
	client := s3.NewFromConfig(cfg, WithS3Retryer)
	assert.IsType(t, &retry.AdaptiveMode{}, client.Options().Retryer)
}

func TestS3ClientWaitsForRetryAfter(t *testing.T) {
	const wait = time.Second
	var attempts atomic.Int32
	var firstAttempt, secondAttempt time.Time
	client := newTestS3Client(t, aws.RetryModeStandard, func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			firstAttempt = time.Now()
			w.Header().Set("Retry-After", strconv.Itoa(int(wait/time.Second)))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		secondAttempt = time.Now()
		w.Header().Set("Content-Range", "bytes 0-4/5")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("hello"))
	})

	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
		Range:  aws.String("bytes=0-4"),
	})
	require.NoError(t, err)
	out.Body.Close()
	assert.Equal(t, int32(2), attempts.Load())
	assert.GreaterOrEqual(t, secondAttempt.Sub(firstAttempt), wait)
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"absent", "", 0},
		{"seconds", "15", 15 * time.Second},
		{"zero", "0", 0},
		{"negative", "-5", 0},
		{"seconds over the cap", "86400", MaxRetryAfter},
		{"seconds that overflow a duration", "9223372036854775807", MaxRetryAfter},
		{"http date", now.Add(20 * time.Second).Format(http.TimeFormat), 20 * time.Second},
		{"http date in the past", now.Add(-time.Minute).Format(http.TimeFormat), 0},
		{"http date over the cap", now.Add(time.Hour).Format(http.TimeFormat), MaxRetryAfter},
		{"garbage", "soon", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{}
			if test.header != "" {
				header.Set("Retry-After", test.header)
			}
			err := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     header,
			}}}
			assert.Equal(t, test.want, retryAfter(err, now))
		})
	}

	assert.Equal(t, time.Duration(0), retryAfter(errors.New("connection reset"), now))
	assert.Equal(t, time.Duration(0), retryAfter(nil, now))
}

func TestBackoffDelayUsesLongerRetryAfter(t *testing.T) {
	err := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"15"}},
	}}}
	delay, derr := backoffDelay(1, err)
	require.NoError(t, derr)
	assert.Equal(t, 15*time.Second, delay)
}

func TestIsTooManyRequests(t *testing.T) {
	throttled := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusTooManyRequests}}}
	unavailable := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}}
	assert.Equal(t, aws.TrueTernary, isTooManyRequests(throttled))
	assert.Equal(t, aws.UnknownTernary, isTooManyRequests(unavailable))
	assert.Equal(t, aws.UnknownTernary, isTooManyRequests(errors.New("other")))
}

func TestS3ClientGivesUpAfterCount(t *testing.T) {
	var attempts atomic.Int32
	client := newTestS3Client(t, aws.RetryModeStandard, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}, fastRetries)

	_, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	require.Error(t, err)
	assert.Equal(t, int32(Count+1), attempts.Load())
}

func TestBackoffDelayFollowsNewBackOff(t *testing.T) {
	jitter := NewBackOff().RandomizationFactor
	reference := NewBackOff()
	reference.RandomizationFactor = 0
	for attempt := 1; attempt <= 20; attempt++ {
		expected := reference.NextBackOff()
		low := time.Duration(float64(expected) * (1 - jitter))
		high := time.Duration(float64(expected) * (1 + jitter))
		delay, err := backoffDelay(attempt, nil)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, delay, low, "attempt %d", attempt)
		assert.LessOrEqual(t, delay, high, "attempt %d", attempt)
	}
}
