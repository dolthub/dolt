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

package credentialhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http/httpguts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
)

const (
	getCommand  = "get"
	refreshSkew = 30 * time.Second
)

var reservedHeaders = map[string]struct{}{
	"authorization":     {},
	"connection":        {},
	"content-length":    {},
	"content-type":      {},
	"host":              {},
	"proxy-connection":  {},
	"te":                {},
	"trailer":           {},
	"transfer-encoding": {},
	"upgrade":           {},
	"user-agent":        {},
}

type getCredentialsRequest struct {
	URI string `json:"uri"`
}

type getCredentialsResponse struct {
	Headers map[string][]string `json:"headers"`
	Expires *time.Time          `json:"expires"`
}

type getCredentialsFunc func(context.Context, string, string) (getCredentialsResponse, error)

// Helper obtains and caches headers for one remotesapi origin.
// It follows Bazel's credential helper protocol so existing helpers can be
// adapted without inventing another secret exchange format.
type Helper struct {
	executable string
	origin     string
	get        getCredentialsFunc
	now        func() time.Time

	mu      sync.Mutex
	headers http.Header
	expires time.Time
}

// New returns a helper for a single canonical remotesapi origin.
func New(executable, origin string) (*Helper, error) {
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return nil, fmt.Errorf("credential helper executable cannot be empty")
	}

	origin, err := canonicalOrigin(origin)
	if err != nil {
		return nil, err
	}

	return &Helper{
		executable: executable,
		origin:     origin,
		get:        runCredentialHelper,
		now:        time.Now,
	}, nil
}

// DialOptions returns interceptors which attach helper headers to unary and
// streaming RPCs without replacing Dolt's existing per-RPC credentials.
func (h *Helper) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(h.unaryClientInterceptor),
		grpc.WithChainStreamInterceptor(h.streamClientInterceptor),
	}
}

// WrapHTTPFetcher adds helper headers only when a remotesapi request uses the
// same origin. In particular, credentials must not follow signed URLs to an
// object store returned by the remotesapi server.
func (h *Helper) WrapHTTPFetcher(fetcher grpcendpoint.HTTPFetcher) grpcendpoint.HTTPFetcher {
	return helperHTTPFetcher{
		fetcher: fetcher,
		helper:  h,
	}
}

func (h *Helper) unaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx, err := h.withOutgoingMetadata(ctx)
	if err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func (h *Helper) streamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	ctx, err := h.withOutgoingMetadata(ctx)
	if err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func (h *Helper) withOutgoingMetadata(ctx context.Context) (context.Context, error) {
	headers, err := h.getHeaders(ctx)
	if err != nil {
		return nil, err
	}

	existing, _ := metadata.FromOutgoingContext(ctx)
	keyValues := make([]string, 0, len(headers)*2)
	for name, values := range headers {
		if _, ok := existing[name]; ok {
			return nil, fmt.Errorf("credential helper header %q conflicts with existing gRPC metadata", name)
		}
		for _, value := range values {
			keyValues = append(keyValues, name, value)
		}
	}

	return metadata.AppendToOutgoingContext(ctx, keyValues...), nil
}

func (h *Helper) getHeaders(ctx context.Context) (http.Header, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now()
	if h.headers != nil && now.Add(refreshSkew).Before(h.expires) {
		return cloneHeaders(h.headers), nil
	}

	response, err := h.get(ctx, h.executable, h.origin)
	if err != nil {
		return nil, err
	}
	now = h.now()

	headers, err := normalizeHeaders(response.Headers)
	if err != nil {
		return nil, err
	}

	h.headers = nil
	h.expires = time.Time{}
	if response.Expires != nil {
		if !response.Expires.After(now) {
			return nil, fmt.Errorf("credential helper returned credentials that expired at %s", response.Expires.Format(time.RFC3339))
		}
		if now.Add(refreshSkew).Before(*response.Expires) {
			h.headers = cloneHeaders(headers)
			h.expires = *response.Expires
		}
	}

	return headers, nil
}

func runCredentialHelper(ctx context.Context, executable, origin string) (getCredentialsResponse, error) {
	request, err := json.Marshal(getCredentialsRequest{URI: origin})
	if err != nil {
		return getCredentialsResponse{}, err
	}

	cmd := exec.CommandContext(ctx, executable, getCommand)
	cmd.Stdin = bytes.NewReader(request)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return getCredentialsResponse{}, fmt.Errorf("credential helper %q failed: %w: %s", executable, err, message)
		}
		return getCredentialsResponse{}, fmt.Errorf("credential helper %q failed: %w", executable, err)
	}

	var response getCredentialsResponse
	decoder := json.NewDecoder(&stdout)
	if err := decoder.Decode(&response); err != nil {
		return getCredentialsResponse{}, fmt.Errorf("credential helper %q returned invalid JSON: %w", executable, err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return getCredentialsResponse{}, fmt.Errorf("credential helper %q returned more than one JSON value", executable)
		}
		return getCredentialsResponse{}, fmt.Errorf("credential helper %q returned invalid trailing data: %w", executable, err)
	}

	return response, nil
}

func normalizeHeaders(headers map[string][]string) (http.Header, error) {
	normalized := make(http.Header, len(headers))
	for name, values := range headers {
		lowerName := strings.ToLower(name)
		if !httpguts.ValidHeaderFieldName(lowerName) {
			return nil, fmt.Errorf("credential helper returned invalid header name %q", name)
		}
		if strings.HasPrefix(lowerName, "grpc-") {
			return nil, fmt.Errorf("credential helper cannot set reserved gRPC header %q", name)
		}
		if _, ok := reservedHeaders[lowerName]; ok {
			return nil, fmt.Errorf("credential helper cannot set reserved header %q", name)
		}
		if _, ok := normalized[lowerName]; ok {
			return nil, fmt.Errorf("credential helper returned duplicate header %q", name)
		}

		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return nil, fmt.Errorf("credential helper returned an invalid value for header %q", name)
			}
			normalized[lowerName] = append(normalized[lowerName], value)
		}
	}
	return normalized, nil
}

func canonicalOrigin(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid credential helper origin %q: %w", rawURL, err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("credential helper origin must use http or https, got %q", parsed.Scheme)
	}

	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", fmt.Errorf("credential helper origin %q has no host", rawURL)
	}

	port := parsed.Port()
	if port == "" {
		if scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	}

	return (&url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
	}).String(), nil
}

func cloneHeaders(headers http.Header) http.Header {
	cloned := make(http.Header, len(headers))
	for name, values := range headers {
		cloned[name] = append([]string(nil), values...)
	}
	return cloned
}

type helperHTTPFetcher struct {
	fetcher grpcendpoint.HTTPFetcher
	helper  *Helper
}

func (f helperHTTPFetcher) Do(req *http.Request) (*http.Response, error) {
	origin, err := canonicalOrigin(req.URL.String())
	if err != nil {
		return nil, err
	}
	if origin != f.helper.origin {
		return f.fetcher.Do(req)
	}

	headers, err := f.helper.getHeaders(req.Context())
	if err != nil {
		return nil, err
	}

	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	if cloned.Header == nil {
		cloned.Header = make(http.Header)
	}
	for name, values := range headers {
		if hasHeader(cloned.Header, name) {
			return nil, fmt.Errorf("credential helper header %q conflicts with existing HTTP header", name)
		}
		for _, value := range values {
			cloned.Header.Add(name, value)
		}
	}
	return f.fetcher.Do(cloned)
}

func hasHeader(headers http.Header, name string) bool {
	for existing := range headers {
		if strings.EqualFold(existing, name) {
			return true
		}
	}
	return false
}
