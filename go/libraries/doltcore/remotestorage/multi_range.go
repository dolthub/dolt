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
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/cenkalti/backoff/v4"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/reliable"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/netstats"
)

// Multi-range fetch tunables, read once at package init.
//
// When enabled, the chunk fetcher replaces the Tree + single-range
// HTTP path with an MRQueue + multi-range HTTP path. Multiple byte
// ranges are packed into a single HTTP request's Range header; the
// server is expected to return a multipart/byteranges response.
// Suitable for CloudFront fronts in front of S3 — raw S3 does not
// support multi-range.
var (
	multiRangeEnabled   = os.Getenv("DOLT_MULTI_RANGE") != ""
	multiRangeSlop      = uint64(envIntOr("DOLT_MULTI_RANGE_SLOP", 256))
	multiRangeMaxBytes  = uint64(envIntOr("DOLT_MULTI_RANGE_MAX_BYTES", 4*1024*1024))
	multiRangeMaxRanges = envIntOr("DOLT_MULTI_RANGE_MAX_RANGES", 128)
)

// MultiRangeEnabled reports whether DOLT_MULTI_RANGE is set.
func MultiRangeEnabled() bool { return multiRangeEnabled }

// MultiRangeParams returns the tunables for the current process.
// Exposed so the chunk-fetcher dispatch layer can use the same slop
// value when populating the MRQueue.
func MultiRangeParams() (slop, maxBytes uint64, maxRanges int) {
	return multiRangeSlop, multiRangeMaxBytes, multiRangeMaxRanges
}

// GetMultiRangeDownloadFunc returns a downloader that packs gr.Ranges
// into a single multi-range HTTP request (comma-separated Range
// header), parses the multipart/byteranges response, and delivers
// each chunk via resCb.
//
// Groups for the Range header are derived from gr.Ranges by
// slop-bridging adjacent spans (gap <= multiRangeSlop). MRQueue
// uses the same slop when picking chunks, so the grouping here
// matches exactly.
func (gr *GetRange) GetMultiRangeDownloadFunc(ctx context.Context, stats StatsRecorder, health reliable.HealthRecorder, fetcher HTTPFetcher, params NetworkRequestParams, resCb func(context.Context, []byte, *Range) error, pathToUrl resourcePathToUrlFunc) func() error {
	if len(gr.Ranges) == 0 {
		return func() error { return nil }
	}
	groups := groupRangesBySlop(gr.Ranges, multiRangeSlop)
	rangeHeader := buildMultiRangeHeader(groups)

	return func() error {
		var lastError error
		op := func() (rerr error) {
			defer func() { lastError = rerr }()

			urlS, err := pathToUrl(ctx, lastError, gr.ResourcePath())
			if err != nil {
				return err
			}
			if urlS == "" {
				urlS = gr.Url
			}
			if UseHTTP2 {
				parsed, perr := url.Parse(urlS)
				if perr != nil {
					return backoff.Permanent(perr)
				}
				if parsed.Host == "dolthub-chunks-dev.s3.us-east-1.amazonaws.com" {
					parsed.Host = "dyqg1o7zazd96.cloudfront.net"
					urlS = parsed.String()
				}
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlS, nil)
			if err != nil {
				return backoff.Permanent(err)
			}
			req.Header.Set("Range", rangeHeader)

			resp, err := fetcher.Do(req)
			if err != nil {
				health.RecordFailure()
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// The server returned the whole object: our
				// Range was ignored. For multi-range this is
				// treated as an error — the caller must either
				// switch off multi-range for this URL or fail.
				health.RecordFailure()
				return backoff.Permanent(fmt.Errorf("server returned 200 to multi-range request; multi-range likely unsupported, URL: %s, RangeHeader: %s, Content-Length: %v", urlS, rangeHeader, resp.ContentLength))
			}
			if resp.StatusCode != http.StatusPartialContent {
				health.RecordFailure()
				return fmt.Errorf("%w: %d", reliable.ErrHttpStatus, resp.StatusCode)
			}

			ct := resp.Header.Get("Content-Type")
			mediaType, ctParams, _ := mime.ParseMediaType(ct)
			if mediaType == "multipart/byteranges" {
				boundary := ctParams["boundary"]
				if boundary == "" {
					health.RecordFailure()
					return backoff.Permanent(errors.New("multipart/byteranges response missing boundary"))
				}
				err = parseMultipartAndDeliver(ctx, resp.Body, boundary, groups, resCb)
				if err != nil {
					health.RecordFailure()
					return err
				}
			} else {
				// 206 with a single body. Legal when only one
				// range was asked for.
				if len(groups) != 1 {
					health.RecordFailure()
					return fmt.Errorf("expected multipart/byteranges for %d groups, got content-type %q", len(groups), ct)
				}
				err = deliverGroupFromReader(ctx, resp.Body, groups[0], resCb)
				if err != nil {
					health.RecordFailure()
					return err
				}
			}

			health.RecordSuccess()
			if netstats.Enabled() {
				netstats.Global().RecordMultiRangeRequest(uint64(len(groups)), uint64(len(gr.Ranges)))
			}
			return nil
		}

		return backoff.Retry(op, downloadBackOff(ctx, params.DownloadRetryCount))
	}
}

// groupRangesBySlop walks ranges (assumed to be in ascending offset
// order within one URL) and groups runs of chunks whose inter-chunk
// gaps are all <= slop into the same group. This produces the same
// grouping MRQueue.PopRequest produces with the same slop.
func groupRangesBySlop(ranges []*Range, slop uint64) [][]*Range {
	if len(ranges) == 0 {
		return nil
	}
	var groups [][]*Range
	cur := []*Range{ranges[0]}
	curEnd := ranges[0].Offset + uint64(ranges[0].Length)
	for _, r := range ranges[1:] {
		gap := r.Offset - curEnd
		if gap <= slop {
			cur = append(cur, r)
		} else {
			groups = append(groups, cur)
			cur = []*Range{r}
		}
		curEnd = r.Offset + uint64(r.Length)
	}
	groups = append(groups, cur)
	return groups
}

// buildMultiRangeHeader formats groups as a Range header value:
// "bytes=A-B,C-D,..." where each group spans [group[0].Offset,
// last chunk end).
func buildMultiRangeHeader(groups [][]*Range) string {
	var b strings.Builder
	b.WriteString("bytes=")
	for i, g := range groups {
		if i > 0 {
			b.WriteByte(',')
		}
		start := g[0].Offset
		last := g[len(g)-1]
		end := last.Offset + uint64(last.Length) - 1
		fmt.Fprintf(&b, "%d-%d", start, end)
	}
	return b.String()
}

// deliverGroupFromReader streams bytes from r (which must carry
// exactly the group's span: [group[0].Offset, last chunk end)) and
// delivers each chunk to resCb, discarding slop bytes in between.
func deliverGroupFromReader(ctx context.Context, r io.Reader, group []*Range, resCb func(context.Context, []byte, *Range) error) error {
	if len(group) == 0 {
		return nil
	}
	start := group[0].Offset
	// pos is the number of bytes already consumed from r, relative
	// to the group's start offset.
	var pos uint64
	for i, rang := range group {
		gap := (rang.Offset - start) - pos
		if gap > 0 {
			if _, err := io.CopyN(io.Discard, r, int64(gap)); err != nil {
				return fmt.Errorf("skip slop before chunk %d: %w", i, err)
			}
			pos += gap
		}
		buf := make([]byte, rang.Length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("read chunk %d: %w", i, err)
		}
		if err := resCb(ctx, buf, rang); err != nil {
			return err
		}
		pos += uint64(rang.Length)
	}
	return nil
}

// parseMultipartAndDeliver reads a multipart/byteranges response,
// matches each part to a group by its Content-Range start offset, and
// hands the part body to deliverGroupFromReader. Parts may arrive in
// any order (the RFC does not require request order).
func parseMultipartAndDeliver(ctx context.Context, r io.Reader, boundary string, groups [][]*Range, resCb func(context.Context, []byte, *Range) error) error {
	mr := multipart.NewReader(r, boundary)

	byStart := make(map[uint64]int, len(groups))
	for i, g := range groups {
		byStart[g[0].Offset] = i
	}
	seen := make([]bool, len(groups))

	matched := 0
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading multipart: %w", err)
		}
		cr := part.Header.Get("Content-Range")
		start, end, ok := parseContentRange(cr)
		if !ok {
			part.Close()
			return fmt.Errorf("malformed Content-Range: %q", cr)
		}
		gi, ok := byStart[start]
		if !ok {
			part.Close()
			return fmt.Errorf("no matching group for Content-Range: %q", cr)
		}
		if seen[gi] {
			part.Close()
			return fmt.Errorf("duplicate part for group %d (Content-Range %q)", gi, cr)
		}
		// Cheap sanity check: group's expected end should match
		// the part's end.
		group := groups[gi]
		last := group[len(group)-1]
		expectedEnd := last.Offset + uint64(last.Length) - 1
		if end != expectedEnd {
			part.Close()
			return fmt.Errorf("Content-Range end %d != expected %d for group %d", end, expectedEnd, gi)
		}
		seen[gi] = true
		err = deliverGroupFromReader(ctx, part, group, resCb)
		part.Close()
		if err != nil {
			return err
		}
		matched++
	}
	if matched != len(groups) {
		return fmt.Errorf("multipart response covered %d of %d requested groups", matched, len(groups))
	}
	return nil
}

// parseContentRange parses values of the form "bytes A-B/C" (or
// "bytes A-B/*") and returns (A, B, true). Returns ok=false when the
// input does not match that shape.
func parseContentRange(s string) (start, end uint64, ok bool) {
	const prefix = "bytes "
	if !strings.HasPrefix(s, prefix) {
		return 0, 0, false
	}
	s = s[len(prefix):]
	slash := strings.Index(s, "/")
	if slash < 0 {
		return 0, 0, false
	}
	rng := s[:slash]
	dash := strings.Index(rng, "-")
	if dash < 0 {
		return 0, 0, false
	}
	a, err := strconv.ParseUint(rng[:dash], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	b, err := strconv.ParseUint(rng[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return a, b, true
}
