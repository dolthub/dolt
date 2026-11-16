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

package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for the S3-compatible blobstore. They run only when
// TEST_S3_BUCKET is set, following the convention of the GCS/OCI tests.
// TEST_S3_ENDPOINT (optional) points them at an S3-compatible provider such
// as Cloudflare R2 or MinIO; TEST_S3_PATH_STYLE=true forces path-style
// addressing. Credentials come from the standard AWS SDK chain.
const (
	envTestS3Bucket    = "TEST_S3_BUCKET"
	envTestS3Endpoint  = "TEST_S3_ENDPOINT"
	envTestS3PathStyle = "TEST_S3_PATH_STYLE"
)

func newTestS3Blobstore(t *testing.T) *S3Blobstore {
	bucket := os.Getenv(envTestS3Bucket)
	if bucket == "" {
		t.Skipf("skipping: %s not set", envTestS3Bucket)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := os.Getenv(envTestS3Endpoint); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
		if os.Getenv(envTestS3PathStyle) == "true" {
			o.UsePathStyle = true
		}
	})

	return NewS3Blobstore(client, bucket, "dolt-s3-blobstore-test/"+uuid.New().String())
}

func TestS3PutGetExists(t *testing.T) {
	bs := newTestS3Blobstore(t)
	ctx := context.Background()

	data := []byte("s3 blobstore test payload")
	ver, err := PutBytes(ctx, bs, "obj", data)
	require.NoError(t, err)
	assert.NotEmpty(t, ver)

	ok, err := bs.Exists(ctx, "obj")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = bs.Exists(ctx, "absent")
	require.NoError(t, err)
	assert.False(t, ok)

	read, gotVer, err := GetBytes(ctx, bs, "obj", AllRange)
	require.NoError(t, err)
	assert.Equal(t, data, read)
	assert.Equal(t, ver, gotVer)

	// ranged read: bytes 3..7
	rangeRead, _, err := GetBytes(ctx, bs, "obj", NewBlobRange(3, 4))
	require.NoError(t, err)
	assert.Equal(t, data[3:7], rangeRead)

	_, _, err = GetBytes(ctx, bs, "absent", AllRange)
	assert.True(t, IsNotFoundError(err))
}

func TestS3CheckAndPutManifest(t *testing.T) {
	bs := newTestS3Blobstore(t)
	ctx := context.Background()

	// create with no expected version
	v1, err := bs.CheckAndPutManifest(ctx, "", []byte("one1"))
	require.NoError(t, err)
	require.NotEmpty(t, v1)

	// create again must fail: key exists
	_, err = bs.CheckAndPutManifest(ctx, "", []byte("two2"))
	assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)

	// update with the correct version succeeds
	v2, err := bs.CheckAndPutManifest(ctx, v1, []byte("two2"))
	require.NoError(t, err)
	require.NotEmpty(t, v2)
	require.NotEqual(t, v1, v2)

	// update with a stale version fails
	_, err = bs.CheckAndPutManifest(ctx, v1, []byte("three3"))
	assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)

	// If-Match against a missing key fails and must not create it
	_, err = bs.CheckAndPutManifest(ctx, v1, []byte("nope"))
	assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)
	ok, err := bs.Exists(ctx, "no-such-key")
	require.NoError(t, err)
	assert.False(t, ok)

	// the winning contents survived
	read, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	assert.Equal(t, []byte("two2"), read)
}

func TestS3CheckAndPutManifestConcurrent(t *testing.T) {
	bs := newTestS3Blobstore(t)
	ctx := context.Background()

	base, err := bs.CheckAndPutManifest(ctx, "", []byte("base"))
	require.NoError(t, err)

	const racers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, conflicts := 0, 0

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf("racer-%02d", n))
			_, err := bs.CheckAndPutManifest(ctx, base, body)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
			} else if IsCheckAndPutError(err) {
				conflicts++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, successes, "exactly one racer must win")
	assert.Equal(t, racers-1, conflicts, "every loser must see CheckAndPutError")
}

func TestS3Concatenate(t *testing.T) {
	bs := newTestS3Blobstore(t)
	_, err := bs.Concatenate(context.Background(), "cat", []string{"a", "b"})
	assert.Error(t, err)
}

func TestS3LargePut(t *testing.T) {
	bs := newTestS3Blobstore(t)
	ctx := context.Background()

	// large enough to cross the upload manager's default multipart threshold (5MiB parts)
	size := 6 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	ver, err := bs.Put(ctx, "big", int64(size), bytes.NewReader(data))
	require.NoError(t, err)
	assert.NotEmpty(t, ver)

	rc, sz, _, err := bs.Get(ctx, "big", AllRange)
	require.NoError(t, err)
	defer rc.Close()
	assert.Equal(t, uint64(size), sz)
	read, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, read)
}
