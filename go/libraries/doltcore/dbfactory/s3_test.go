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

package dbfactory

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
)

func TestParseS3Url(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		bucket  string
		prefix  string
		routing s3Routing
	}{
		{
			name:   "bucket and database only",
			url:    "s3://bucket/db",
			bucket: "bucket",
			prefix: "/db",
		},
		{
			name:   "nested prefix",
			url:    "s3://bucket/team/db",
			bucket: "bucket",
			prefix: "/team/db",
		},
		{
			name:    "r2 style endpoint and region",
			url:     "s3://bucket/db?endpoint=https://account.r2.cloudflarestorage.com&region=auto",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{endpoint: "https://account.r2.cloudflarestorage.com", region: "auto"},
		},
		{
			name:    "minio style path addressing",
			url:     "s3://bucket/db?endpoint=http://localhost:9000&path-style=true",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{endpoint: "http://localhost:9000", pathStyle: true},
		},
		{
			name:    "path style can be turned off explicitly",
			url:     "s3://bucket/db?path-style=false",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{pathStyle: false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			urlObj, err := earl.Parse(test.url)
			require.NoError(t, err)

			bucket, prefix, routing, err := parseS3Url(urlObj)
			require.NoError(t, err)
			assert.Equal(t, test.bucket, bucket)
			assert.Equal(t, test.prefix, prefix)
			assert.Equal(t, test.routing, routing)
		})
	}
}

func TestParseS3UrlErrors(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		errLike string
	}{
		{
			name:    "no bucket",
			url:     "s3:///db",
			errLike: "s3://bucket/path",
		},
		{
			name:    "credentials in the url are refused",
			url:     "s3://AKID:secret@bucket/db",
			errLike: "must not embed credentials",
		},
		{
			name:    "unknown parameter",
			url:     "s3://bucket/db?entpoint=https://example.com",
			errLike: "unknown s3 url parameter",
		},
		{
			name:    "path-style must be a bool",
			url:     "s3://bucket/db?path-style=yes",
			errLike: "must be true or false",
		},
		{
			name:    "empty value",
			url:     "s3://bucket/db?endpoint=",
			errLike: "needs exactly one non-empty value",
		},
		{
			name:    "repeated parameter is ambiguous",
			url:     "s3://bucket/db?region=auto&region=us-east-1",
			errLike: "needs exactly one non-empty value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateS3Url(test.url)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errLike)
		})
	}
}

// A url that survives validation must survive the round trip through the
// remote's stored url string, since that is how routing is persisted.
func TestValidateS3UrlAcceptsRoutedUrls(t *testing.T) {
	urls := []string{
		"s3://bucket/db",
		"s3://bucket/db?region=auto",
		"s3://bucket/db?endpoint=https://account.r2.cloudflarestorage.com&region=auto&path-style=true",
	}

	for _, u := range urls {
		assert.NoError(t, ValidateS3Url(u), u)
	}
}

func TestS3ClientRetriesBareTooManyRequests(t *testing.T) {
	const throttledAttempts = 3
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= throttledAttempts {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Range", "bytes 0-4/5")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
	}, withRemoteStorageRetryer, func(o *s3.Options) {
		o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, time.Millisecond)
	})

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
}

func TestS3ClientGivesUpAfterRemoteStorageRetryCount(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
	}, withRemoteStorageRetryer, func(o *s3.Options) {
		o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, time.Millisecond)
	})

	_, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	require.Error(t, err)
	assert.Equal(t, int32(remotestorage.NetworkRetryCount+1), attempts.Load())
}

func TestRemoteStorageBackoffDelayFollowsNetworkBackOff(t *testing.T) {
	jitter := remotestorage.NewNetworkBackOff().RandomizationFactor
	reference := remotestorage.NewNetworkBackOff()
	reference.RandomizationFactor = 0
	for attempt := 1; attempt <= 20; attempt++ {
		expected := reference.NextBackOff()
		low := time.Duration(float64(expected) * (1 - jitter))
		high := time.Duration(float64(expected) * (1 + jitter))
		delay, err := remoteStorageBackoffDelay(attempt, nil)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, delay, low, "attempt %d", attempt)
		assert.LessOrEqual(t, delay, high, "attempt %d", attempt)
	}
}
