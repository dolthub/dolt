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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type UrlFactoryFunc func(error) (string, error)

type StreamingResponse struct {
	Body   io.Reader
	cancel func()
}

func (r StreamingResponse) Close() error {
	r.cancel()
	return nil
}

type StatsRecorder interface {
	RecordTimeToFirstByte(retry int, size uint64, d time.Duration)
	RecordDownloadAttemptStart(retry int, offset, size uint64)
	RecordDownloadComplete(retry int, size uint64, d time.Duration)
}

type HealthRecorder interface {
	RecordSuccess()
	RecordFailure()
}

var ErrThroughputTooLow = errors.New("throughput below minimum threshold")
var ErrHttpStatus = errors.New("http status")

type MinimumThroughputCheck struct {
	CheckInterval time.Duration
	BytesPerCheck int
	NumIntervals  int
}

type BackOffFactory func(context.Context) backoff.BackOff

type StreamingRangeRequest struct {
	Fetcher     HTTPFetcher
	Offset      uint64
	Length      uint64
	UrlFact     UrlFactoryFunc
	Stats       StatsRecorder
	Health      HealthRecorder
	BackOffFact BackOffFactory
	Throughput  MinimumThroughputCheck
}

// |StreamingRangeDownload| makes an immediate GET request to the URL returned
// from |urlStrF|, returning a |StreamingResponse| object which can be used to
// consume the body of the response. A |StreamingResponse| should be |Close|d
// by the consumer, and it is safe to do so before the entire response has been
// read, if a condition arises where the response is no longer needed.
//
// This method will kick off a goroutine which is responsible for making the
// HTTP request(s) associated with fulfilling the request. Only one HTTP
// request will be inflight at a time, but if errors are encountered while
// making the requests or reading the response body, further requests may be
// made for as-yet-undelivered bytes from the requested byte range.
//
// As a result, the bytes read from |StreamingResponse.Body| may be the
// concatenation of multiple requests made the URLs returned from |urlStrF|.
// Thus, those URLs should represent the same immutable remote resource which
// is guaranteed to return the same bytes for overlapping byte ranges.
//
// If there is a fatal error when making the requests, it will be delivered
// through the |err| responses of the |Read| method on
// |StreamingResponse.Read|.
//
// |StreamingResponse.Read| can (and often will) return short reads.
func StreamingRangeDownload(ctx context.Context, req StreamingRangeRequest) StreamingResponse {
	rangeEnd := req.Offset + req.Length - 1
	r, w := io.Pipe()

	// This is the overall context for the opreation, encompassing all of its retries. When StreamingResponse is closed, this is canceled.
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		origOffset := req.Offset
		offset := req.Offset
		var retry int
		var lastError error
		op := func() (rerr error) {
			defer func() { retry += 1 }()
			defer func() { lastError = rerr }()
			// This is the per-call context. It can be canceled by
			// EnforceThroughput, for example, without canceling
			// the entire operation.
			ctx, cCause := context.WithCancelCause(ctx)

			url, err := req.UrlFact(lastError)
			if err != nil {
				return err
			}

			httpReq, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			rangeHeaderVal := fmt.Sprintf("bytes=%d-%d", offset, rangeEnd)
			httpReq.Header.Set("Range", rangeHeaderVal)

			req.Stats.RecordDownloadAttemptStart(retry, offset-origOffset, req.Length)
			start := time.Now()
			resp, err := req.Fetcher.Do(httpReq.WithContext(ctx))
			if err != nil {
				req.Health.RecordFailure()
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				req.Health.RecordFailure()
				return fmt.Errorf("%w: %d", ErrHttpStatus, resp.StatusCode)
			}
			req.Stats.RecordTimeToFirstByte(retry, req.Length, time.Since(start))

			reader := &AtomicCountingReader{r: resp.Body}
			cleanup := EnforceThroughput(reader.Count, req.Throughput, func(err error) {
				cCause(err)
			})
			n, err := io.Copy(w, reader)
			cleanup()
			offset += uint64(n)
			if err == nil {
				// Success!
				req.Health.RecordSuccess()
				return nil
			} else if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.ErrShortWrite) {
				// Reader closed; bail.
				return backoff.Permanent(err)
			} else {
				if cerr := context.Cause(ctx); errors.Is(err, context.Canceled) && cerr != nil {
					err = cerr
				}
				// Let backoff decide when and if we retry.
				req.Health.RecordFailure()
				return err
			}
		}
		start := time.Now()
		err := backoff.Retry(op, req.BackOffFact(ctx))
		if err != nil {
			w.CloseWithError(err)
		} else {
			req.Stats.RecordDownloadComplete(retry, req.Length, time.Since(start))
			w.Close()
		}
	}()

	return StreamingResponse{
		Body:   r,
		cancel: cancel,
	}
}

type AtomicCountingReader struct {
	r io.Reader
	c atomic.Uint64
}

func (r *AtomicCountingReader) Read(bs []byte) (int, error) {
	n, err := r.r.Read(bs)
	r.c.Add(uint64(n))
	return n, err
}

func (r *AtomicCountingReader) Count() uint64 {
	return r.c.Load()
}

// EnforceThroughput will spawn a naked goroutine that will watch a |cnt|
// source. If the rate by which |cnt| is increasing drops below the configured
// threshold for too long, it will call |cancel|.  EnforceThroughput should be
// cleaned up by calling |cleanup| once whatever it is monitoring is finished.
func EnforceThroughput(cnt func() uint64, params MinimumThroughputCheck, cancel func(error)) (cleanup func()) {
	done := make(chan struct{})
	go func() {
		n := params.NumIntervals
		var counts []uint64
		// Note: We don't look at the clock when we take these
		// observations. If we make late observations, then we may see
		// higher numbers than we should have and think our throughput
		// is higher than it is.
		tooSlow := func() bool {
			if len(counts) < n {
				return false
			}
			copy(counts[:n], counts[len(counts)-n:])
			counts = counts[:n]
			cnt := counts[n-1] - counts[0]
			if int(cnt) < params.BytesPerCheck*n {
				return true
			}
			return false
		}
		for {
			select {
			case <-time.After(params.CheckInterval):
				counts = append(counts, cnt())
				if tooSlow() {
					cancel(fmt.Errorf("%w: needed %d bytes per interval across %d intervals, went from %d to %d instead",
						ErrThroughputTooLow, params.BytesPerCheck, n, counts[0], counts[n-1]))
					return
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
	}
}
