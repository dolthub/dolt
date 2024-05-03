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

	"github.com/fatih/color"

	"github.com/cenkalti/backoff/v4"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
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

// We may need this to be configurable for users with really bad internet
var downThroughputCheck = iohelp.MinThroughputCheckParams{
	MinBytesPerSec: 1024,
	CheckInterval:  1 * time.Second,
	NumIntervals:   5,
}

const (
	downRetryCount = 5
)

type StatsRecorder interface {
	RecordTimeToFirstByte(retry int, size uint64, d time.Duration)
	RecordDownloadAttemptStart(retry int, offset, size uint64)
	RecordDownloadComplete(retry int, size uint64, d time.Duration)
}

type HealthRecorder interface {
	RecordSuccess()
	RecordFailure()
}

func downloadBackOff(ctx context.Context) backoff.BackOff {
	ret := backoff.NewExponentialBackOff()
	ret.MaxInterval = 5 * time.Second
	return backoff.WithContext(backoff.WithMaxRetries(ret, downRetryCount), ctx)
}

var ErrThroughputTooLow = errors.New("throughput below minimum threshold")
var ErrHttpStatus = errors.New("http status")

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
func StreamingRangeDownload(ctx context.Context, stats StatsRecorder, health HealthRecorder, fetcher HTTPFetcher, offset, length uint64, urlStrF UrlFactoryFunc) StreamingResponse {
	rangeEnd := offset + length - 1
	r, w := io.Pipe()

	// This is the overall context for the opreation, encompassing all of its retries. When StreamingResponse is closed, this is canceled.
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		origOffset := offset
		offset := offset
		var retry int
		op := func() error {
			defer func() { retry += 1 }()
			// This is the per-call context. It can be canceled by
			// EnforceThroughput, for example, without canceling
			// the entire operation.
			ctx, cCause := context.WithCancelCause(ctx)

			url, err := urlStrF(nil)
			if err != nil {
				return err
			}

			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			rangeHeaderVal := fmt.Sprintf("bytes=%d-%d", offset, rangeEnd)
			req.Header.Set("Range", rangeHeaderVal)

			stats.RecordDownloadAttemptStart(retry, offset-origOffset, length)
			start := time.Now()
			resp, err := fetcher.Do(req.WithContext(ctx))
			if err != nil {
				fmt.Fprintf(color.Error, color.RedString("error from HTTP Do: %v\n", err))
				health.RecordFailure()
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				health.RecordFailure()
				fmt.Fprintf(color.Error, color.RedString("error from HTTP StatusCode: %v\n", resp.StatusCode))
				return fmt.Errorf("%w: %d", ErrHttpStatus, resp.StatusCode)
			}
			stats.RecordTimeToFirstByte(retry, length, time.Since(start))

			reader := &AtomicCountingReader{r: resp.Body}
			cleanup := EnforceThroughput(reader.Count, downThroughputCheck, func(err error) {
				cCause(err)
			})
			n, err := io.Copy(w, reader)
			cleanup()
			offset += uint64(n)
			if err == nil {
				// Success!
				health.RecordSuccess()
				return nil
			} else if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.ErrShortWrite) {
				// Reader closed; bail.
				return backoff.Permanent(err)
			} else {
				if cerr := context.Cause(ctx); errors.Is(err, context.Canceled) && cerr != nil {
					err = cerr
				}
				// Let backoff decide when and if we retry.
				fmt.Fprintf(color.Error, color.RedString("error from io.Copy: %v\n", err))
				health.RecordFailure()
				return err
			}
		}
		start := time.Now()
		err := backoff.Retry(op, downloadBackOff(ctx))
		if err != nil {
			fmt.Fprintf(color.Error, color.RedString("error from backoff.Retry: %v\n", err))
			w.CloseWithError(err)
		} else {
			stats.RecordDownloadComplete(retry, length, time.Since(start))
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
func EnforceThroughput(cnt func() uint64, params iohelp.MinThroughputCheckParams, cancel func(error)) (cleanup func()) {
	done := make(chan struct{})
	go func() {
		n := params.NumIntervals
		targetPerMilli := uint64(params.MinBytesPerSec) / uint64(time.Second/time.Millisecond)
		type point struct {
			c uint64
			t time.Time
		}
		var points []point
		var cntPerMilli uint64
		tooSlow := func() bool {
			if len(points) < n {
				return false
			}
			copy(points[:n], points[len(points)-n:])
			points = points[:n]
			dur := points[n-1].t.Sub(points[0].t)
			cnt := points[n-1].c - points[0].c
			cntPerMilli = cnt / uint64(dur/time.Millisecond)
			if cntPerMilli < targetPerMilli {
				return true
			}
			return false
		}
		for {
			select {
			case <-time.After(params.CheckInterval):
				points = append(points, point{cnt(), time.Now()})
				if tooSlow() {
					cancel(fmt.Errorf("%w: needed %d bytes per milli, got %d; %d bytes at %v, %d bytes at %v",
						ErrThroughputTooLow, targetPerMilli, cntPerMilli, points[0].c, points[0].t,
						points[n-1].c, points[n-1].t))
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
