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

// Package netstats records network byte counters at three layers —
// gRPC RPC payloads, HTTP request/response bodies, and the underlying TCP
// connection — to help investigate where bytes are going during remote
// operations like dolt fetch.
//
// Enable by setting DOLT_NETSTATS to a non-empty value. Stats are dumped
// at process exit, on SIGUSR1, and periodically if DOLT_NETSTATS_INTERVAL
// is set to a Go duration (e.g. "5s").
package netstats

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/stats"
)

var (
	initOnce sync.Once
	enabled  bool
	global   *Recorder
)

// Enabled returns true if DOLT_NETSTATS is set. Initialization of the global
// recorder, signal handler, and optional periodic dumper happens on first call.
func Enabled() bool {
	initOnce.Do(func() {
		enabled = os.Getenv("DOLT_NETSTATS") != ""
		if !enabled {
			return
		}
		global = newRecorder()
		global.startSignalHandler()
		if d := os.Getenv("DOLT_NETSTATS_INTERVAL"); d != "" {
			if dur, err := time.ParseDuration(d); err == nil && dur > 0 {
				go global.runPeriodicDump(dur)
			} else {
				fmt.Fprintf(os.Stderr, "netstats: ignoring invalid DOLT_NETSTATS_INTERVAL=%q: %v\n", d, err)
			}
		}
	})
	return enabled
}

// Global returns the process-wide Recorder, or nil if netstats is disabled.
func Global() *Recorder {
	Enabled()
	return global
}

type methodStats struct {
	calls               atomic.Uint64
	inHeaderBytes       atomic.Uint64
	inPayloadBytes      atomic.Uint64
	inPayloadWireBytes  atomic.Uint64
	inPayloadCount      atomic.Uint64
	outPayloadBytes     atomic.Uint64
	outPayloadWireBytes atomic.Uint64
	outPayloadCount     atomic.Uint64
	inTrailerBytes      atomic.Uint64
}

type targetStats struct {
	requests        atomic.Uint64
	reqHeaderBytes  atomic.Uint64
	reqBodyBytes    atomic.Uint64
	respHeaderBytes atomic.Uint64
	respBodyBytes   atomic.Uint64
	statuses        sync.Map // int -> *atomic.Uint64
}

type connStats struct {
	conns atomic.Uint64
	in    atomic.Uint64
	out   atomic.Uint64
}

// Recorder aggregates network counters across gRPC, HTTP, and TCP layers.
// All methods are safe for concurrent use.
type Recorder struct {
	start time.Time

	grpcMu       sync.Mutex
	grpcByMethod map[string]*methodStats

	httpMu       sync.Mutex
	httpByTarget map[string]*targetStats

	tcpMu         sync.Mutex
	tcpByAddr     map[string]*connStats
	tcpTotalIn    atomic.Uint64
	tcpTotalOut   atomic.Uint64
	tcpTotalConns atomic.Uint64
}

func newRecorder() *Recorder {
	return &Recorder{
		start:        time.Now(),
		grpcByMethod: map[string]*methodStats{},
		httpByTarget: map[string]*targetStats{},
		tcpByAddr:    map[string]*connStats{},
	}
}

// ---- gRPC stats.Handler ----

// StatsHandler returns a grpc/stats.Handler that records per-method byte
// counters. Attach to a client conn with grpc.WithStatsHandler.
func (r *Recorder) StatsHandler() stats.Handler { return (*grpcHandler)(r) }

type grpcHandler Recorder

type grpcMethodKey struct{}

func (h *grpcHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, grpcMethodKey{}, info.FullMethodName)
}

func (h *grpcHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	method, _ := ctx.Value(grpcMethodKey{}).(string)
	if method == "" {
		method = "unknown"
	}
	r := (*Recorder)(h)
	m := r.getMethodStats(method)
	switch v := s.(type) {
	case *stats.Begin:
		m.calls.Add(1)
	case *stats.InHeader:
		m.inHeaderBytes.Add(uint64(v.WireLength))
	case *stats.InPayload:
		m.inPayloadBytes.Add(uint64(v.Length))
		m.inPayloadWireBytes.Add(uint64(v.WireLength))
		m.inPayloadCount.Add(1)
	case *stats.OutPayload:
		m.outPayloadBytes.Add(uint64(v.Length))
		m.outPayloadWireBytes.Add(uint64(v.WireLength))
		m.outPayloadCount.Add(1)
	case *stats.InTrailer:
		m.inTrailerBytes.Add(uint64(v.WireLength))
	}
}

func (*grpcHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*grpcHandler) HandleConn(_ context.Context, _ stats.ConnStats) {}

func (r *Recorder) getMethodStats(method string) *methodStats {
	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()
	m, ok := r.grpcByMethod[method]
	if !ok {
		m = &methodStats{}
		r.grpcByMethod[method] = m
	}
	return m
}

// ---- HTTP RoundTripper ----

// WrapRoundTripper wraps base so that request/response headers and response
// body bytes are counted, bucketed by URL scheme+host. If base is nil,
// http.DefaultTransport is used.
func (r *Recorder) WrapRoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &countingRoundTripper{base: base, r: r}
}

type countingRoundTripper struct {
	base http.RoundTripper
	r    *Recorder
}

// CloseIdleConnections forwards to the wrapped RoundTripper if it
// supports the convention. This lets callers (e.g. PerURLFetcher)
// release idle conns even through the stats wrapper.
func (c *countingRoundTripper) CloseIdleConnections() {
	if ci, ok := c.base.(interface{ CloseIdleConnections() }); ok {
		ci.CloseIdleConnections()
	}
}

func (c *countingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t := c.r.getTargetStats(targetKey(req.URL))
	t.requests.Add(1)
	t.reqHeaderBytes.Add(uint64(approxReqHeaderBytes(req)))
	if req.ContentLength > 0 {
		t.reqBodyBytes.Add(uint64(req.ContentLength))
	}
	resp, err := c.base.RoundTrip(req)
	if err != nil || resp == nil {
		return resp, err
	}
	cv, _ := t.statuses.LoadOrStore(resp.StatusCode, new(atomic.Uint64))
	cv.(*atomic.Uint64).Add(1)
	t.respHeaderBytes.Add(uint64(approxRespHeaderBytes(resp)))
	resp.Body = &countingReadCloser{rc: resp.Body, cnt: &t.respBodyBytes}
	return resp, nil
}

func (r *Recorder) getTargetStats(target string) *targetStats {
	r.httpMu.Lock()
	defer r.httpMu.Unlock()
	t, ok := r.httpByTarget[target]
	if !ok {
		t = &targetStats{}
		r.httpByTarget[target] = t
	}
	return t
}

func targetKey(u *url.URL) string {
	if u == nil {
		return "unknown"
	}
	return u.Scheme + "://" + u.Host
}

// approxReqHeaderBytes estimates what HTTP/1.1 would have sent on the wire.
// Exact HTTP/2 HPACK-compressed sizes are not available to user code; this
// is a serviceable upper bound for accounting.
func approxReqHeaderBytes(req *http.Request) int {
	n := len(req.Method) + 1 + len(req.URL.RequestURI()) + 1 + len("HTTP/1.1") + 2
	if req.Host != "" {
		n += len("Host: ") + len(req.Host) + 2
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			n += len(k) + 2 + len(v) + 2
		}
	}
	n += 2 // blank line
	return n
}

func approxRespHeaderBytes(resp *http.Response) int {
	n := len("HTTP/1.1") + 1 + 3 + 1 + len(resp.Status) + 2
	for k, vs := range resp.Header {
		for _, v := range vs {
			n += len(k) + 2 + len(v) + 2
		}
	}
	n += 2
	return n
}

type countingReadCloser struct {
	rc  io.ReadCloser
	cnt *atomic.Uint64
}

func (c *countingReadCloser) Read(b []byte) (int, error) {
	n, err := c.rc.Read(b)
	if n > 0 {
		c.cnt.Add(uint64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error { return c.rc.Close() }

// ---- net.Dialer wrapping ----

// DialContextFunc matches (*net.Dialer).DialContext.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// WrapDialContext wraps a DialContext function so that bytes read from and
// written to each returned net.Conn are counted. This captures raw TCP
// byte volume (including TLS framing overhead) inside this process.
func (r *Recorder) WrapDialContext(base DialContextFunc) DialContextFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := base(ctx, network, addr)
		if err != nil {
			return c, err
		}
		cs := r.getConnStats(addr)
		cs.conns.Add(1)
		r.tcpTotalConns.Add(1)
		return &countingConn{Conn: c, s: cs, total: r}, nil
	}
}

func (r *Recorder) getConnStats(addr string) *connStats {
	r.tcpMu.Lock()
	defer r.tcpMu.Unlock()
	s, ok := r.tcpByAddr[addr]
	if !ok {
		s = &connStats{}
		r.tcpByAddr[addr] = s
	}
	return s
}

type countingConn struct {
	net.Conn
	s     *connStats
	total *Recorder
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.s.in.Add(uint64(n))
		c.total.tcpTotalIn.Add(uint64(n))
	}
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.s.out.Add(uint64(n))
		c.total.tcpTotalOut.Add(uint64(n))
	}
	return n, err
}

// ---- Dump / periodic ----

// Dump writes a human-readable summary of all recorded counters.
func (r *Recorder) Dump(w io.Writer) {
	var buf strings.Builder
	fmt.Fprintf(&buf, "\n=== dolt netstats (elapsed %s) ===\n", time.Since(r.start).Round(time.Millisecond))

	fmt.Fprintf(&buf, "TCP: conns=%d  in=%s  out=%s\n",
		r.tcpTotalConns.Load(),
		humanBytes(r.tcpTotalIn.Load()),
		humanBytes(r.tcpTotalOut.Load()))

	r.tcpMu.Lock()
	addrs := make([]string, 0, len(r.tcpByAddr))
	for a := range r.tcpByAddr {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		s := r.tcpByAddr[a]
		fmt.Fprintf(&buf, "  %s  conns=%d  in=%s  out=%s\n",
			a, s.conns.Load(), humanBytes(s.in.Load()), humanBytes(s.out.Load()))
	}
	r.tcpMu.Unlock()

	fmt.Fprintln(&buf, "gRPC (client):")
	r.grpcMu.Lock()
	methods := make([]string, 0, len(r.grpcByMethod))
	for m := range r.grpcByMethod {
		methods = append(methods, m)
	}
	sort.Strings(methods)
	for _, m := range methods {
		s := r.grpcByMethod[m]
		fmt.Fprintf(&buf, "  %s\n", m)
		fmt.Fprintf(&buf, "    calls=%d  in_msgs=%d  out_msgs=%d\n",
			s.calls.Load(), s.inPayloadCount.Load(), s.outPayloadCount.Load())
		fmt.Fprintf(&buf, "    in:  hdr=%s  payload=%s (wire %s)  trailer=%s\n",
			humanBytes(s.inHeaderBytes.Load()),
			humanBytes(s.inPayloadBytes.Load()),
			humanBytes(s.inPayloadWireBytes.Load()),
			humanBytes(s.inTrailerBytes.Load()))
		fmt.Fprintf(&buf, "    out: payload=%s (wire %s)\n",
			humanBytes(s.outPayloadBytes.Load()),
			humanBytes(s.outPayloadWireBytes.Load()))
	}
	r.grpcMu.Unlock()

	fmt.Fprintln(&buf, "HTTP:")
	r.httpMu.Lock()
	targets := make([]string, 0, len(r.httpByTarget))
	for t := range r.httpByTarget {
		targets = append(targets, t)
	}
	sort.Strings(targets)
	for _, target := range targets {
		t := r.httpByTarget[target]
		fmt.Fprintf(&buf, "  %s\n", target)
		fmt.Fprintf(&buf, "    reqs=%d  req(hdr=%s body=%s)  resp(hdr=%s body=%s)",
			t.requests.Load(),
			humanBytes(t.reqHeaderBytes.Load()), humanBytes(t.reqBodyBytes.Load()),
			humanBytes(t.respHeaderBytes.Load()), humanBytes(t.respBodyBytes.Load()))
		var codes []int
		t.statuses.Range(func(k, _ any) bool {
			codes = append(codes, k.(int))
			return true
		})
		sort.Ints(codes)
		if len(codes) > 0 {
			parts := make([]string, 0, len(codes))
			for _, code := range codes {
				v, _ := t.statuses.Load(code)
				parts = append(parts, fmt.Sprintf("%d=%d", code, v.(*atomic.Uint64).Load()))
			}
			fmt.Fprintf(&buf, "  [%s]", strings.Join(parts, ","))
		}
		fmt.Fprintln(&buf)
	}
	r.httpMu.Unlock()
	fmt.Fprintln(&buf, "=== end netstats ===")

	_, _ = io.WriteString(w, buf.String())
}

func (r *Recorder) runPeriodicDump(d time.Duration) {
	t := time.NewTicker(d)
	defer t.Stop()
	for range t.C {
		r.Dump(os.Stderr)
	}
}

func humanBytes(n uint64) string {
	const k = 1024.0
	f := float64(n)
	switch {
	case f < k:
		return fmt.Sprintf("%dB", n)
	case f < k*k:
		return fmt.Sprintf("%.1fKiB", f/k)
	case f < k*k*k:
		return fmt.Sprintf("%.1fMiB", f/(k*k))
	case f < k*k*k*k:
		return fmt.Sprintf("%.2fGiB", f/(k*k*k))
	default:
		return fmt.Sprintf("%.3fTiB", f/(k*k*k*k))
	}
}
