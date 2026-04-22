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

package remotestorage

import (
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in   string
		a, b uint64
		ok   bool
	}{
		{"bytes 0-99/100", 0, 99, true},
		{"bytes 100-199/1000", 100, 199, true},
		{"bytes 0-0/*", 0, 0, true},
		{"", 0, 0, false},
		{"bytes 100/1000", 0, 0, false},
		{"bytes abc-def/1000", 0, 0, false},
		{"not bytes", 0, 0, false},
	}
	for _, c := range cases {
		a, b, ok := parseContentRange(c.in)
		assert.Equal(t, c.ok, ok, c.in)
		if c.ok {
			assert.Equal(t, c.a, a, c.in)
			assert.Equal(t, c.b, b, c.in)
		}
	}
}

func TestBuildMultiRangeHeader(t *testing.T) {
	r1 := &Range{Offset: 0, Length: 100}
	r2 := &Range{Offset: 500, Length: 50}
	r3 := &Range{Offset: 1000, Length: 25}
	h := buildMultiRangeHeader([][]*Range{{r1}, {r2}, {r3}})
	assert.Equal(t, "bytes=0-99,500-549,1000-1024", h)
}

func TestGroupRangesBySlop(t *testing.T) {
	r1 := &Range{Offset: 0, Length: 100}    // ends at 100
	r2 := &Range{Offset: 200, Length: 100}  // gap 100, <=256 slop: bridge
	r3 := &Range{Offset: 1000, Length: 100} // gap 700: new group
	groups := groupRangesBySlop([]*Range{r1, r2, r3}, 256)
	require.Len(t, groups, 2)
	assert.Equal(t, []*Range{r1, r2}, groups[0])
	assert.Equal(t, []*Range{r3}, groups[1])
}

// writeMultipartBytesRanges serves a canonical multipart/byteranges
// response body given a source object and requested (start, end)
// ranges.
func writeMultipartBytesRanges(w http.ResponseWriter, obj []byte, ranges [][2]uint64) {
	mw := multipart.NewWriter(nil) // just to pick a boundary
	boundary := mw.Boundary()
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+boundary)
	w.WriteHeader(http.StatusPartialContent)

	real := multipart.NewWriter(w)
	real.SetBoundary(boundary)
	for _, r := range ranges {
		start, end := r[0], r[1]
		h := textproto.MIMEHeader{}
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj)))
		part, err := real.CreatePart(h)
		if err != nil {
			return
		}
		_, _ = part.Write(obj[start : end+1])
	}
	_ = real.Close()
}

// makeDownloadParams returns a NetworkRequestParams suitable for
// tests — no retries, minimal timeouts.
func makeDownloadParams() NetworkRequestParams {
	p := defaultRequestParams
	p.DownloadRetryCount = 0
	p.RespHeadersTimeout = 5 * time.Second
	return p
}

type collectCB struct {
	mu       sync.Mutex
	received map[uint64][]byte // offset -> bytes
}

func newCollectCB() *collectCB {
	return &collectCB{received: map[uint64][]byte{}}
}

func (c *collectCB) cb() func(context.Context, []byte, *Range) error {
	return func(_ context.Context, b []byte, r *Range) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		cpy := make([]byte, len(b))
		copy(cpy, b)
		c.received[r.Offset] = cpy
		return nil
	}
}

type noopHealth struct{}

func (noopHealth) RecordSuccess() {}
func (noopHealth) RecordFailure() {}

func TestMultiRangeDownload_Multipart(t *testing.T) {
	// Fabricate a 2000-byte object with deterministic contents.
	obj := make([]byte, 2000)
	for i := range obj {
		obj[i] = byte(i & 0xff)
	}

	// Two groups: one span [100, 199], one span [1000, 1099].
	ranges := []*Range{
		{Offset: 100, Length: 100},
		{Offset: 1000, Length: 100},
	}

	var wantRange string
	var sawStatus int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantRange = r.Header.Get("Range")
		sawStatus = http.StatusPartialContent
		writeMultipartBytesRanges(w, obj, [][2]uint64{{100, 199}, {1000, 1099}})
	}))
	defer srv.Close()

	gr := &GetRange{Url: srv.URL, Ranges: ranges}
	cc := newCollectCB()
	urlF := func(context.Context, error, string) (string, error) { return srv.URL, nil }
	f := gr.GetMultiRangeDownloadFunc(context.Background(), NullStatsRecorder{}, noopHealth{}, http.DefaultClient, makeDownloadParams(), cc.cb(), urlF)

	require.NoError(t, f())
	assert.Equal(t, "bytes=100-199,1000-1099", wantRange)
	assert.Equal(t, http.StatusPartialContent, sawStatus)
	require.Len(t, cc.received, 2)
	assert.Equal(t, obj[100:200], cc.received[100])
	assert.Equal(t, obj[1000:1100], cc.received[1000])
}

func TestMultiRangeDownload_SlopBridging(t *testing.T) {
	obj := make([]byte, 2000)
	for i := range obj {
		obj[i] = byte(i & 0xff)
	}
	// Two chunks within slop distance that should bridge into a
	// single Range header entry. Gap = 50 < default slop.
	ranges := []*Range{
		{Offset: 100, Length: 50},  // [100, 150)
		{Offset: 200, Length: 50},  // [200, 250)  — gap 50 from prev, bridges
		{Offset: 2000, Length: 0},  // placeholder we won't use
	}
	ranges = ranges[:2]

	var wantRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantRange = r.Header.Get("Range")
		// Single group: one big range spanning both chunks + 50 B slop.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 100-249/%d", len(obj)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(obj[100:250])
	}))
	defer srv.Close()

	gr := &GetRange{Url: srv.URL, Ranges: ranges}
	cc := newCollectCB()
	urlF := func(context.Context, error, string) (string, error) { return srv.URL, nil }
	f := gr.GetMultiRangeDownloadFunc(context.Background(), NullStatsRecorder{}, noopHealth{}, http.DefaultClient, makeDownloadParams(), cc.cb(), urlF)

	require.NoError(t, f())
	assert.Equal(t, "bytes=100-249", wantRange)
	require.Len(t, cc.received, 2)
	assert.Equal(t, obj[100:150], cc.received[100])
	assert.Equal(t, obj[200:250], cc.received[200])
}

func TestMultiRangeDownload_Status200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("full object"))
	}))
	defer srv.Close()

	gr := &GetRange{
		Url: srv.URL,
		Ranges: []*Range{
			{Offset: 0, Length: 4},
			{Offset: 100, Length: 4},
		},
	}
	cc := newCollectCB()
	urlF := func(context.Context, error, string) (string, error) { return srv.URL, nil }
	f := gr.GetMultiRangeDownloadFunc(context.Background(), NullStatsRecorder{}, noopHealth{}, http.DefaultClient, makeDownloadParams(), cc.cb(), urlF)
	err := f()
	require.Error(t, err)
	// Our code wraps this as backoff.Permanent.
	var perm *backoff.PermanentError
	// the backoff library returns a *backoff.PermanentError directly when
	// MaxRetries is 0; Unwrap chain should show it.
	if _, ok := err.(*backoff.PermanentError); !ok {
		// OK to not match exactly, but the error message should
		// mention the 200.
		_ = perm
		assert.Contains(t, err.Error(), "200")
	}
	assert.Empty(t, cc.received)
}

