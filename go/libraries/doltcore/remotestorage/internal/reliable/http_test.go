// Copyright 2024 Dolthub, Inc.
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

package reliable

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnforceThroughput(t *testing.T) {
	t.Run("CancelShutsDown", func(t *testing.T) {
		var didCancel atomic.Bool
		before := runtime.NumGoroutine()
		c := EnforceThroughput(func() uint64 { return 0 }, MinimumThroughputCheck{
			CheckInterval: time.Hour,
			BytesPerCheck: 1024,
			NumIntervals:  16,
		}, func(error) { didCancel.Store(true) })
		start := time.Now()
		for before >= runtime.NumGoroutine() && time.Since(start) < time.Second {
		}
		assert.Greater(t, runtime.NumGoroutine(), before)
		c()
		start = time.Now()
		for before < runtime.NumGoroutine() && time.Since(start) < time.Second {
		}
		assert.Equal(t, before, runtime.NumGoroutine())
		assert.False(t, didCancel.Load())
	})
	t.Run("DoesNotCancelThenCancels", func(t *testing.T) {
		var i, c int
		cnt := func() uint64 {
			if i > 64 {
				i += 1
				return uint64(c)
			}
			c += 128
			i += 1
			return uint64(c)
		}
		done := make(chan struct{})
		cleanup := EnforceThroughput(cnt, MinimumThroughputCheck{
			CheckInterval: time.Millisecond,
			BytesPerCheck: 64,
			NumIntervals:  16,
		}, func(error) { close(done) })
		t.Cleanup(cleanup)

		select {
		case <-done:
			assert.Greater(t, i, 64)
		case <-time.After(time.Second * 3):
			assert.FailNow(t, "EnforceThroughput did not cancel operation after 3 seconds.")
		}
	})
}

type fetcherFunc func(*http.Request) (*http.Response, error)

func (f fetcherFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

type countingHealth struct {
	successes atomic.Int64
	failures  atomic.Int64
}

func (c *countingHealth) RecordSuccess() { c.successes.Add(1) }
func (c *countingHealth) RecordFailure() { c.failures.Add(1) }

type noopStats struct{}

func (noopStats) RecordTimeToFirstByte(retry int, size uint64, d time.Duration)   {}
func (noopStats) RecordDownloadAttemptStart(retry int, offset, size uint64)       {}
func (noopStats) RecordDownloadComplete(retry int, size uint64, d time.Duration)  {}

// http2LikeBody simulates an HTTP/2 response body where the transport has
// delivered all content bytes but has not yet observed the END_STREAM frame.
// After the payload is drained, subsequent Read calls block until the request
// context is canceled, at which point they return context.Canceled — the same
// behavior Go's http2 transport exhibits in that situation.
type http2LikeBody struct {
	buf *bytes.Reader
	ctx context.Context
}

func (b *http2LikeBody) Read(p []byte) (int, error) {
	if b.buf.Len() > 0 {
		return b.buf.Read(p)
	}
	<-b.ctx.Done()
	return 0, context.Canceled
}

func (b *http2LikeBody) Close() error { return nil }

func TestStreamingRangeDownload(t *testing.T) {
	t.Run("ConsumerCloseAfterFullReadIsNotAFailure", func(t *testing.T) {
		// Regression: With HTTP/2, Body.Read may not return io.EOF
		// until an END_STREAM frame arrives. If the consumer closes
		// the StreamingResponse immediately after reading the last
		// byte, io.Copy on the producer side returns context.Canceled
		// even though the transfer succeeded. That must not be
		// recorded as a health failure.
		const size = 1024
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i)
		}

		fetcher := fetcherFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusPartialContent,
				Body: &http2LikeBody{
					buf: bytes.NewReader(payload),
					ctx: req.Context(),
				},
				Request: req,
			}, nil
		})

		health := &countingHealth{}
		resp := StreamingRangeDownload(context.Background(), StreamingRangeRequest{
			Fetcher: fetcher,
			Offset:  0,
			Length:  size,
			UrlFact: func(error) (string, error) { return "http://example.test/file", nil },
			Stats:   noopStats{},
			Health:  health,
			BackOffFact: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
			Throughput: MinimumThroughputCheck{
				CheckInterval: time.Hour,
				BytesPerCheck: 1,
				NumIntervals:  1,
			},
			RespHeadersTimeout: time.Hour,
		})

		got := make([]byte, size)
		_, err := io.ReadFull(resp.Body, got)
		require.NoError(t, err)
		require.Equal(t, payload, got)

		require.NoError(t, resp.Close())

		// Give the producer goroutine a moment to unwind after
		// cancel() and either record its (non-)outcome.
		start := time.Now()
		for health.successes.Load() == 0 && health.failures.Load() == 0 && time.Since(start) < 2*time.Second {
			time.Sleep(time.Millisecond)
		}

		assert.Equal(t, int64(0), health.failures.Load(), "consumer close after full read must not record a failure")
		assert.Equal(t, int64(1), health.successes.Load(), "consumer close after full read should record one success")
	})

	t.Run("EarlyConsumerCloseDoesNotRecordFailure", func(t *testing.T) {
		// Sanity check of the existing behavior: if the consumer
		// closes before the full range is delivered, io.Copy fails
		// with io.ErrClosedPipe and we take the backoff.Permanent
		// path — no health signal is recorded (neither success nor
		// failure).
		const size = 1024
		payload := make([]byte, size)

		fetcher := fetcherFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusPartialContent,
				Body:       io.NopCloser(bytes.NewReader(payload)),
				Request:    req,
			}, nil
		})

		health := &countingHealth{}
		resp := StreamingRangeDownload(context.Background(), StreamingRangeRequest{
			Fetcher: fetcher,
			Offset:  0,
			Length:  size,
			UrlFact: func(error) (string, error) { return "http://example.test/file", nil },
			Stats:   noopStats{},
			Health:  health,
			BackOffFact: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
			Throughput: MinimumThroughputCheck{
				CheckInterval: time.Hour,
				BytesPerCheck: 1,
				NumIntervals:  1,
			},
			RespHeadersTimeout: time.Hour,
		})

		// Read just one byte, then close.
		one := make([]byte, 1)
		_, err := io.ReadFull(resp.Body, one)
		require.NoError(t, err)
		require.NoError(t, resp.Close())

		// Let the producer unwind.
		start := time.Now()
		for health.successes.Load() == 0 && health.failures.Load() == 0 && time.Since(start) < 500*time.Millisecond {
			time.Sleep(time.Millisecond)
		}

		assert.Equal(t, int64(0), health.failures.Load(), "early consumer close must not record a failure")
	})
}
