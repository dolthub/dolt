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
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Blobstore provides an S3-compatible implementation of the Blobstore
// interface. It works against any object store that implements the S3 API
// including conditional writes (If-Match / If-None-Match on PutObject):
// AWS S3, Cloudflare R2, and MinIO among others.
//
// Version tokens are object ETags, treated as opaque strings and passed back
// to the server exactly as received. CheckAndPut relies on conditional
// PutObject: If-None-Match:* when no version is expected (create), and
// If-Match:<etag> otherwise (replace). A failed precondition (HTTP 412), a
// conditional-write conflict (HTTP 409, AWS "ConditionalRequestConflict"),
// or a missing key under If-Match are all surfaced as CheckAndPutError so
// the NBS manifest layer rereads and retries.
//
// CheckAndPut always uses a direct single-part PutObject: ETags of multipart
// uploads are not content-stable across providers, and conditional headers on
// CompleteMultipartUpload are not universally supported. This is intended for
// small objects such as the NBS manifest. Ordinary Put uses the SDK upload
// manager, which switches to multipart automatically for large objects.
type S3Blobstore struct {
	client     *s3.Client
	uploader   *manager.Uploader
	bucketName string
	prefix     string
}

var _ Blobstore = &S3Blobstore{}

// NewS3Blobstore creates a new instance of an S3Blobstore.
func NewS3Blobstore(client *s3.Client, bucketName, prefix string) *S3Blobstore {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	uploader := manager.NewUploader(client)
	return &S3Blobstore{client, uploader, bucketName, prefix}
}

func (bs *S3Blobstore) Path() string {
	return path.Join(bs.bucketName, bs.prefix)
}

func (bs *S3Blobstore) Teardown(ctx context.Context) error {
	return nil
}

func (bs *S3Blobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := path.Join(bs.prefix, key)
	_, err := bs.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bs.bucketName),
		Key:    aws.String(absKey),
	})
	if err == nil {
		return true, nil
	}
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) || s3HTTPStatus(err) == 404 {
		return false, nil
	}
	return false, err
}

func (bs *S3Blobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	absKey := path.Join(bs.prefix, key)
	req := &s3.GetObjectInput{
		Bucket: aws.String(bs.bucketName),
		Key:    aws.String(absKey),
	}

	byteRange := br.asHttpRangeHeader()
	if byteRange != "" {
		req.Range = aws.String(byteRange)
	}

	res, err := bs.client.GetObject(ctx, req)
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &noSuchKey) || s3HTTPStatus(err) == 404 {
			return nil, 0, "", NotFound{"s3://" + path.Join(bs.bucketName, absKey)}
		}
		return nil, 0, "", err
	}

	var size uint64
	// For range requests the total size comes from the Content-Range header.
	if res.ContentRange != nil {
		size = parseContentRangeSize(*res.ContentRange)
	}
	if size == 0 && res.ContentLength != nil {
		size = uint64(*res.ContentLength)
	}

	// handle negative offset and positive length
	if br.offset < 0 && br.length > 0 {
		lr := io.LimitReader(res.Body, br.length)
		return struct {
			io.Reader
			io.Closer
		}{lr, res.Body}, size, fmtstr(res.ETag), nil
	}

	return res.Body, size, fmtstr(res.ETag), nil
}

// Large objects are uploaded via multipart automatically.
func (bs *S3Blobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	res, err := bs.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bs.bucketName),
		Key:    aws.String(absKey),
		Body:   reader,
	})
	if err != nil {
		return "", err
	}
	return fmtstr(res.ETag), nil
}

func (bs *S3Blobstore) CheckAndPutManifest(ctx context.Context, expectedVersion string, contents []byte) (string, error) {
	absKey := path.Join(bs.prefix, ManifestKey)
	req := &s3.PutObjectInput{
		Bucket:        aws.String(bs.bucketName),
		Key:           aws.String(absKey),
		Body:          bytes.NewReader(contents),
		ContentLength: aws.Int64(int64(len(contents))),
	}

	if expectedVersion != "" {
		req.IfMatch = aws.String(expectedVersion)
	} else {
		req.IfNoneMatch = aws.String("*")
	}

	res, err := bs.client.PutObject(ctx, req)
	if err != nil {
		status := s3HTTPStatus(err)
		switch {
		case status == 412:
			// Precondition failed: another writer won the race.
			return "", CheckAndPutError{Key: ManifestKey, ExpectedVersion: expectedVersion, ActualVersion: "unknown"}
		case isS3ConditionalConflict(err):
			// AWS returns 409 ConditionalRequestConflict when concurrent
			// conditional writes collide; the caller must reread and retry,
			// which is exactly the CheckAndPutError contract.
			return "", CheckAndPutError{Key: ManifestKey, ExpectedVersion: expectedVersion, ActualVersion: "unknown"}
		case expectedVersion != "" && status == 404:
			// If-Match against a missing key: the expected version cannot
			// match a nonexistent object.
			return "", CheckAndPutError{Key: ManifestKey, ExpectedVersion: expectedVersion, ActualVersion: ""}
		}
		return "", err
	}

	return fmtstr(res.ETag), nil
}

// Concatenate is unimplemented: generic S3-compatible stores have no
// server-side compose operation. Use an NBS constructor that never conjoins
// through the blobstore (e.g. nbs.NewNoConjoinBSStore).
func (bs *S3Blobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return "", fmt.Errorf("concatenate is unimplemented on the s3 blobstore")
}

// s3HTTPStatus extracts the HTTP status code from an SDK error, or 0.
func s3HTTPStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// isS3ConditionalConflict reports whether err is AWS's conditional-write
// conflict (HTTP 409, code "ConditionalRequestConflict"). Only this specific
// 409 maps to CheckAndPutError; generic 409s do not.
func isS3ConditionalConflict(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "ConditionalRequestConflict"
	}
	return false
}
