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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeS3 implements the subset of the S3 API S3Blobstore uses: conditional
// PutObject, GetObject with ranges, and HeadObject. It runs in process, so
// these tests need no credentials, no network, and no provider.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	etags    map[string]string
	seq      int
	failNext int
	requests int
	lastPut  http.Header
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, etags: map[string]string{}}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests++
	if f.failNext > 0 {
		f.failNext--
		writeS3Error(w, http.StatusInternalServerError, "InternalError")
		return
	}

	key := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(key) != 2 {
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	f.serveKey(w, r, key[1])
}

func (f *fakeS3) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodPut:
		f.put(w, r, key)
	case http.MethodGet, http.MethodHead:
		f.get(w, r, key)
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, key string) {
	f.lastPut = r.Header.Clone()

	cur, exists := f.etags[key]
	if r.Header.Get("If-None-Match") == "*" && exists {
		writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	if im := r.Header.Get("If-Match"); im != "" {
		if !exists {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		if im != cur {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
	}

	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(r.Body); err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError")
		return
	}

	content := body.Bytes()
	if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		decoded, err := decodeAWSChunked(content)
		if err != nil {
			writeS3Error(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		content = decoded
	}

	f.seq++
	etag := fmt.Sprintf("%q", fmt.Sprintf("etag-%d", f.seq))
	f.objects[key] = content
	f.etags[key] = etag

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, key string) {
	data, ok := f.objects[key]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
		return
	}

	start, end := 0, len(data)
	partial := false
	if rng := r.Header.Get("Range"); rng != "" {
		var err error
		start, end, err = parseTestRange(rng, len(data))
		if err != nil {
			writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		partial = true
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
	}

	w.Header().Set("ETag", f.etags[key])
	w.Header().Set("Content-Length", strconv.Itoa(end-start))
	if partial {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if r.Method != http.MethodHead {
		w.Write(data[start:end])
	}
}

// parseTestRange handles the two forms asHttpRangeHeader emits: "bytes=a-b"
// and the suffix form "bytes=-n".
func parseTestRange(hdr string, size int) (int, int, error) {
	spec := strings.TrimPrefix(hdr, "bytes=")
	if strings.HasPrefix(spec, "-") {
		n, err := strconv.Atoi(spec[1:])
		if err != nil {
			return 0, 0, err
		}
		if n > size {
			n = size
		}
		return size - n, size, nil
	}

	parts := strings.SplitN(spec, "-", 2)
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	if len(parts) == 1 || parts[1] == "" {
		return start, size, nil
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	if end+1 > size {
		return start, size, nil
	}
	return start, end + 1, nil
}

// decodeAWSChunked unwraps the aws-chunked framing the SDK applies when it
// streams a body with a trailing checksum. Real S3-compatible servers accept
// and decode it, so the fake must too.
func decodeAWSChunked(b []byte) ([]byte, error) {
	var out []byte
	for {
		i := bytes.Index(b, []byte("\r\n"))
		if i < 0 {
			return out, nil
		}

		sizeLine := string(b[:i])
		if j := strings.IndexByte(sizeLine, ';'); j >= 0 {
			sizeLine = sizeLine[:j]
		}
		n, err := strconv.ParseInt(strings.TrimSpace(sizeLine), 16, 64)
		if err != nil {
			return nil, err
		}

		b = b[i+2:]
		if n == 0 {
			return out, nil
		}
		if int64(len(b)) < n {
			return nil, fmt.Errorf("truncated aws-chunked body")
		}

		out = append(out, b[:n]...)
		b = b[n:]
		if len(b) >= 2 && b[0] == '\r' && b[1] == '\n' {
			b = b[2:]
		}
	}
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", code, code)
}

type fakeCreds struct{}

func (fakeCreds) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "AKID", SecretAccessKey: "SECRET", Source: "fake"}, nil
}

// newFakeS3Blobstore starts |f| behind an http or https listener and returns a
// blobstore backed by a real s3.Client pointed at it. The scheme matters: the
// SDK signs plain-http and TLS requests differently, and only the TLS path can
// fall back to aws-chunked streaming for a body it cannot rewind.
func newFakeS3Blobstore(t *testing.T, f *fakeS3, useTLS bool) *S3Blobstore {
	var srv *httptest.Server
	opts := s3.Options{
		Region:                     "us-east-1",
		Credentials:                fakeCreds{},
		UsePathStyle:               true,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
	}

	if useTLS {
		srv = httptest.NewTLSServer(f)
		opts.HTTPClient = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		}
	} else {
		srv = httptest.NewServer(f)
	}
	t.Cleanup(srv.Close)

	opts.BaseEndpoint = aws.String(srv.URL)
	return NewS3Blobstore(s3.New(opts), "bkt", "pfx")
}

// manifestBody mirrors what blobstoreManifest hands CheckAndPut: a
// *bytes.Buffer, which is not an io.Seeker.
func manifestBody(contents string) *bytes.Buffer {
	buf := bytes.NewBuffer(make([]byte, 64*1024)[:0])
	buf.WriteString(contents)
	return buf
}

func TestS3BlobstoreCheckAndPut(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := "http"
		if useTLS {
			name = "https"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFakeS3()
			bs := newFakeS3Blobstore(t, f, useTLS)

			v1, err := bs.CheckAndPut(ctx, "", "manifest", 4, manifestBody("one1"))
			require.NoError(t, err)
			require.NotEmpty(t, v1)

			// A second create must lose: the key already exists.
			_, err = bs.CheckAndPut(ctx, "", "manifest", 4, manifestBody("two2"))
			assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)

			v2, err := bs.CheckAndPut(ctx, v1, "manifest", 4, manifestBody("two2"))
			require.NoError(t, err)
			require.NotEqual(t, v1, v2)

			// Stale version loses.
			_, err = bs.CheckAndPut(ctx, v1, "manifest", 6, manifestBody("three3"))
			assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)

			// If-Match against a missing key fails without creating it.
			_, err = bs.CheckAndPut(ctx, v1, "absent", 4, manifestBody("nope"))
			assert.True(t, IsCheckAndPutError(err), "expected CheckAndPutError, got %v", err)
			exists, err := bs.Exists(ctx, "absent")
			require.NoError(t, err)
			assert.False(t, exists)

			read, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
			require.NoError(t, err)
			assert.Equal(t, []byte("two2"), read)
			assert.Equal(t, v2, ver)
		})
	}
}

// Over plain http the body must be signed and sized. The SDK cannot compute a
// payload hash for a body it cannot rewind, and it refuses to substitute the
// aws-chunked trailing-checksum encoding without TLS, so an unseekable body
// fails before a request is ever issued.
//
// Over TLS the SDK prefers aws-chunked whenever checksum calculation is
// enabled, seekable body or not, so there is no equivalent assertion to make
// there. Whether every S3-compatible provider accepts those trailers is a
// separate question from this one.
func TestS3BlobstoreCheckAndPutSignsBodyOverPlainHTTP(t *testing.T) {
	f := newFakeS3()
	bs := newFakeS3Blobstore(t, f, false)

	_, err := bs.CheckAndPut(context.Background(), "", "manifest", 17, manifestBody("manifest-contents"))
	require.NoError(t, err)

	assert.NotContains(t, f.lastPut.Get("Content-Encoding"), "aws-chunked")
	assert.Empty(t, f.lastPut.Get("X-Amz-Trailer"))
	assert.NotContains(t, f.lastPut.Get("X-Amz-Content-Sha256"), "STREAMING")
	assert.Equal(t, "17", f.lastPut.Get("Content-Length"))
}

// A transient 5xx must be retried by the SDK rather than surfacing as a hard
// error: the manifest layer only retries CheckAndPutError, so anything else
// fails the caller's push. Retrying requires rewinding the body.
func TestS3BlobstoreCheckAndPutRetriesTransientFailure(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := "http"
		if useTLS {
			name = "https"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFakeS3()
			bs := newFakeS3Blobstore(t, f, useTLS)

			f.failNext = 1
			ver, err := bs.CheckAndPut(ctx, "", "manifest", 4, manifestBody("one1"))
			require.NoError(t, err)
			require.NotEmpty(t, ver)
			assert.Equal(t, 2, f.requests, "expected the failed attempt to be retried")

			read, _, err := GetBytes(ctx, bs, "manifest", AllRange)
			require.NoError(t, err)
			assert.Equal(t, []byte("one1"), read)
		})
	}
}

func TestS3BlobstoreGetRanges(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3()
	bs := newFakeS3Blobstore(t, f, false)

	data := []byte("0123456789abcdef")
	_, err := PutBytes(ctx, bs, "obj", data)
	require.NoError(t, err)

	read, _, err := GetBytes(ctx, bs, "obj", AllRange)
	require.NoError(t, err)
	assert.Equal(t, data, read)

	read, _, err = GetBytes(ctx, bs, "obj", NewBlobRange(3, 4))
	require.NoError(t, err)
	assert.Equal(t, data[3:7], read)

	// Suffix range: newBSTableChunkSource reads table indexes this way.
	read, _, err = GetBytes(ctx, bs, "obj", NewBlobRange(-5, 0))
	require.NoError(t, err)
	assert.Equal(t, data[len(data)-5:], read)

	rc, size, _, err := bs.Get(ctx, "obj", NewBlobRange(2, 3))
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, uint64(len(data)), size, "size must report the whole object, not the range")

	_, _, err = GetBytes(ctx, bs, "absent", AllRange)
	assert.True(t, IsNotFoundError(err), "expected NotFound, got %v", err)
}
