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
)

// newTestS3Client returns a client with WithS3Retryer pointed at |handler|,
// with backoff delays capped so retries run quickly.
func newTestS3Client(t *testing.T, handler http.HandlerFunc) *s3.Client {
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
	}, WithS3Retryer, func(o *s3.Options) {
		o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, time.Millisecond)
	})
}

func TestS3ClientRetriesBareTooManyRequests(t *testing.T) {
	const throttledAttempts = 3
	var attempts atomic.Int32
	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= throttledAttempts {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
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
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))
	assert.Equal(t, int32(throttledAttempts+1), attempts.Load())
}

func TestS3ClientGivesUpAfterCount(t *testing.T) {
	var attempts atomic.Int32
	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	})

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
