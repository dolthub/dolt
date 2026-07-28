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
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestHelperCachesHeadersUntilExpiration(t *testing.T) {
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	calls := 0

	helper := newTestHelper(t, "https://example.com", func(context.Context, string, string) (getCredentialsResponse, error) {
		calls++
		expires := now.Add(time.Hour)
		return getCredentialsResponse{
			Headers: map[string][]string{
				"Proxy-Authorization": {"Bearer token"},
			},
			Expires: &expires,
		}, nil
	})
	helper.now = func() time.Time { return now }

	first, err := helper.getHeaders(context.Background())
	require.NoError(t, err)
	second, err := helper.getHeaders(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"Bearer token"}, first["proxy-authorization"])
	assert.Equal(t, first, second)
	assert.Equal(t, 1, calls)

	now = now.Add(time.Hour - refreshSkew)
	_, err = helper.getHeaders(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestHelperDoesNotCacheHeadersWithoutExpiration(t *testing.T) {
	calls := 0
	helper := newTestHelper(t, "https://example.com", func(context.Context, string, string) (getCredentialsResponse, error) {
		calls++
		return getCredentialsResponse{
			Headers: map[string][]string{"x-test": {"value"}},
		}, nil
	})

	_, err := helper.getHeaders(context.Background())
	require.NoError(t, err)
	_, err = helper.getHeaders(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestHelperCoalescesConcurrentRefreshes(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once

	helper := newTestHelper(t, "https://example.com", func(context.Context, string, string) (getCredentialsResponse, error) {
		calls.Add(1)
		startOnce.Do(func() {
			close(started)
		})
		<-release

		expires := time.Now().Add(time.Hour)
		return getCredentialsResponse{
			Headers: map[string][]string{"x-test": {"value"}},
			Expires: &expires,
		}, nil
	})

	const requestCount = 10
	var wg sync.WaitGroup
	errs := make(chan error, requestCount)
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := helper.getHeaders(context.Background())
			errs <- err
		}()
	}

	<-started
	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestHelperRejectsExpiredCredentials(t *testing.T) {
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	expires := now.Add(-time.Second)
	helper := newTestHelper(t, "https://example.com", func(context.Context, string, string) (getCredentialsResponse, error) {
		return getCredentialsResponse{Expires: &expires}, nil
	})
	helper.now = func() time.Time { return now }

	_, err := helper.getHeaders(context.Background())
	require.ErrorContains(t, err, "expired")
}

func TestHelperChecksExpirationAfterCommandReturns(t *testing.T) {
	now := time.Date(2026, time.June, 24, 12, 0, 0, 0, time.UTC)
	expires := now.Add(time.Minute)
	helper := newTestHelper(t, "https://example.com", func(context.Context, string, string) (getCredentialsResponse, error) {
		now = now.Add(2 * time.Minute)
		return getCredentialsResponse{Expires: &expires}, nil
	})
	helper.now = func() time.Time { return now }

	_, err := helper.getHeaders(context.Background())
	require.ErrorContains(t, err, "expired")
}

func TestRunCredentialHelperProtocol(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper uses a POSIX shell")
	}

	executable := filepath.Join(t.TempDir(), "credential-helper")
	err := os.WriteFile(executable, []byte(`#!/bin/sh
if [ "$1" != "get" ]; then
  echo "expected get command" >&2
  exit 1
fi
request=$(cat)
if [ "$request" != '{"uri":"https://example.com:443"}' ]; then
  echo "unexpected request: $request" >&2
  exit 1
fi
printf '%s' '{"headers":{"Proxy-Authorization":["Bearer proxy-token"]},"expires":"2026-06-24T20:00:00Z"}'
`), 0700)
	require.NoError(t, err)

	response, err := runCredentialHelper(context.Background(), executable, "https://example.com:443")
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer proxy-token"}, response.Headers["Proxy-Authorization"])
	require.NotNil(t, response.Expires)
	assert.Equal(t, "2026-06-24T20:00:00Z", response.Expires.Format(time.RFC3339))
}

func TestGRPCRequestPreservesDoltAuthorization(t *testing.T) {
	helper := newTestHelper(t, "https://example.com", staticResponse(map[string][]string{
		"Proxy-Authorization": {"Bearer proxy-token"},
	}))

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		assert.Equal(t, []string{"Basic dolt-credentials"}, md.Get("authorization"))
		assert.Equal(t, []string{"Bearer proxy-token"}, md.Get("proxy-authorization"))
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(staticRPCCredentials{
			"authorization": "Basic dolt-credentials",
		}),
	}
	dialOptions = append(dialOptions, helper.DialOptions()...)

	conn, err := grpc.DialContext(context.Background(), "bufnet", dialOptions...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestStreamInterceptorAddsHelperMetadata(t *testing.T) {
	helper := newTestHelper(t, "https://example.com", staticResponse(map[string][]string{
		"Proxy-Authorization": {"Bearer proxy-token"},
	}))

	_, err := helper.streamClientInterceptor(
		context.Background(),
		&grpc.StreamDesc{},
		nil,
		"/test.Service/Stream",
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			md, _ := metadata.FromOutgoingContext(ctx)
			assert.Equal(t, []string{"Bearer proxy-token"}, md.Get("proxy-authorization"))
			return nil, nil
		},
	)
	require.NoError(t, err)
}

func TestHTTPFetcherAddsHeadersOnlyToMatchingOrigin(t *testing.T) {
	var requests []*http.Request
	fetcher := fetcherFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req)
		return &http.Response{StatusCode: http.StatusOK}, nil
	})
	helper := newTestHelper(t, "https://example.com", staticResponse(map[string][]string{
		"Proxy-Authorization": {"Bearer proxy-token"},
	}))
	wrapped := helper.WrapHTTPFetcher(fetcher)

	matching, err := http.NewRequest(http.MethodGet, "https://example.com/chunks/1", nil)
	require.NoError(t, err)
	_, err = wrapped.Do(matching)
	require.NoError(t, err)

	signedURL, err := http.NewRequest(http.MethodGet, "https://objects.example.net/chunks/1?signature=abc", nil)
	require.NoError(t, err)
	_, err = wrapped.Do(signedURL)
	require.NoError(t, err)

	require.Len(t, requests, 2)
	assert.Equal(t, "Bearer proxy-token", requests[0].Header.Get("proxy-authorization"))
	assert.Empty(t, requests[1].Header.Get("proxy-authorization"))
	assert.Empty(t, matching.Header.Get("proxy-authorization"))
}

func TestHelperRejectsAuthorizationHeader(t *testing.T) {
	helper := newTestHelper(t, "https://example.com", staticResponse(map[string][]string{
		"Authorization": {"Bearer replacement"},
	}))

	_, err := helper.getHeaders(context.Background())
	require.ErrorContains(t, err, "reserved header")
}

func TestCanonicalOrigin(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://EXAMPLE.com/repository/path", "https://example.com:443"},
		{"http://example.com/path", "http://example.com:80"},
		{"https://example.com:8443/path?query=value", "https://example.com:8443"},
		{"https://[::1]/path", "https://[::1]:443"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual, err := canonicalOrigin(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func newTestHelper(t *testing.T, origin string, get getCredentialsFunc) *Helper {
	t.Helper()

	helper, err := New("test-helper", origin)
	require.NoError(t, err)
	helper.get = get
	return helper
}

func staticResponse(headers map[string][]string) getCredentialsFunc {
	return func(context.Context, string, string) (getCredentialsResponse, error) {
		expires := time.Now().Add(time.Hour)
		return getCredentialsResponse{
			Headers: headers,
			Expires: &expires,
		}, nil
	}
}

type staticRPCCredentials map[string]string

func (c staticRPCCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return c, nil
}

func (staticRPCCredentials) RequireTransportSecurity() bool {
	return false
}

var _ credentials.PerRPCCredentials = staticRPCCredentials{}

type fetcherFunc func(*http.Request) (*http.Response, error)

func (f fetcherFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}
