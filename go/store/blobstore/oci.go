// Copyright 2023 Dolthub, Inc.
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
	"math"
	"net/http"
	"os"
	"path"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/sync/errgroup"
)

// 10MB part size
const minPartSize = 10 * 1024 * 1024
const defaultPartSize = 5 * minPartSize
const maxPartNum = 10000
const defaultBatchSize = 500 * 1024 * 1024
const defaultConcurrentListeners = 5

type toUpload struct {
	b       []byte
	partNum int
}

type uploadFunc func(ctx context.Context, objectName, uploadID string, partNumber int, contentLength int64, reader io.Reader) (objectstorage.CommitMultipartUploadPartDetails, error)

type tempLocalObject struct {
	f    *os.File
	path string
}

var _ io.ReadCloser = &tempLocalObject{}

func (t *tempLocalObject) Read(p []byte) (int, error) {
	return t.f.Read(p)
}

func (t *tempLocalObject) Close() error {
	err := t.f.Close()
	os.Remove(t.path)
	return err
}

// OCIBlobstore provides an OCI implementation of the Blobstore interface
type OCIBlobstore struct {
	provider            common.ConfigurationProvider
	client              objectstorage.ObjectStorageClient
	bucketName          string
	namespace           string
	prefix              string
	concurrentListeners int
}

var _ Blobstore = &OCIBlobstore{}

// NewOCIBlobstore creates a new instance of a OCIBlobstore
func NewOCIBlobstore(ctx context.Context, provider common.ConfigurationProvider, client objectstorage.ObjectStorageClient, bucketName, prefix string) (*OCIBlobstore, error) {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	// Disable timeout to support big file upload/download, default is 60s
	client.HTTPClient = &http.Client{}

	request := objectstorage.GetNamespaceRequest{}
	r, err := client.GetNamespace(ctx, request)
	if err != nil {
		return nil, err
	}

	return &OCIBlobstore{provider, client, bucketName, *r.Value, prefix, defaultConcurrentListeners}, nil
}

func (bs *OCIBlobstore) Path() string {
	return path.Join(bs.bucketName, bs.prefix)
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *OCIBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := path.Join(bs.prefix, key)
	_, err := bs.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
		NamespaceName: &bs.namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
	})
	if err == nil {
		return true, nil
	}
	if serr, ok := common.IsServiceError(err); ok {
		// handle not found code
		if serr.GetHTTPStatusCode() == 404 {
			return false, nil
		}
	}
	return false, err
}

// Get retrieves an io.reader for the portion of a blob specified by br along with its version
func (bs *OCIBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	absKey := path.Join(bs.prefix, key)
	req := objectstorage.GetObjectRequest{
		NamespaceName: &bs.namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
	}

	byteRange := br.asHttpRangeHeader()
	if byteRange != "" {
		req.Range = &byteRange
	}

	res, err := bs.client.GetObject(ctx, req)
	if err != nil {
		if serr, ok := common.IsServiceError(err); ok {
			// handle not found code
			if serr.GetHTTPStatusCode() == 404 {
				return nil, 0, "", NotFound{"oci://" + path.Join(bs.bucketName, absKey)}
			}
		}
		return nil, 0, "", err
	}

	var size uint64
	// Try to get total size from Content-Range header first (for range requests)
	if res.RawResponse != nil && res.RawResponse.Header != nil {
		contentRange := res.RawResponse.Header.Get("Content-Range")
		if contentRange != "" {
			size = parseContentRangeSize(contentRange)
		}
	}
	// Fall back to Content-Length if no Content-Range (full object request)
	if size == 0 && res.ContentLength != nil {
		size = uint64(*res.ContentLength)
	}

	// handle negative offset and positive length
	if br.offset < 0 && br.length > 0 {
		lr := io.LimitReader(res.Content, br.length)
		return io.NopCloser(lr), size, fmtstr(res.ETag), nil
	}

	return res.Content, size, fmtstr(res.ETag), nil
}

// Put sets the blob and the version for a key
func (bs *OCIBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return bs.upload(ctx, "", key, totalSize, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *OCIBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	return bs.upload(ctx, expectedVersion, key, totalSize, reader)
}

// At the time of this implementation, Oracle Cloud does not provide a way to create composite objects
// via their APIs/SDKs.
func (bs *OCIBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return "", fmt.Errorf("concatenate is unimplemented on the oci blobstore")
}

func (bs *OCIBlobstore) upload(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	numParts, _ := getNumPartsAndPartSize(totalSize, defaultPartSize, maxPartNum)
	if totalSize == 0 {
		return "", errors.New("failed to upload to oci blobstore, no data in reader")
	} else if totalSize < minPartSize {
		return bs.checkAndPut(ctx, expectedVersion, key, totalSize, reader)
	} else {
		return bs.multipartUpload(ctx, expectedVersion, key, numParts, totalSize, reader)
	}
}

func (bs *OCIBlobstore) checkAndPut(ctx context.Context, expectedVersion, key string, contentLength int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)

	req := objectstorage.PutObjectRequest{
		NamespaceName: &bs.namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
		ContentLength: &contentLength,
		PutObjectBody: io.NopCloser(reader),
	}

	if expectedVersion != "" {
		req.IfMatch = &expectedVersion
	} else {
		star := "*"
		req.IfNoneMatch = &star
	}

	res, err := bs.client.PutObject(ctx, req)
	if err != nil {
		if serr, ok := common.IsServiceError(err); ok {
			if serr.GetHTTPStatusCode() == 412 {
				return "", CheckAndPutError{key, expectedVersion, "unknown (Not supported in OCI implementation)"}
			}
		}
		return "", err
	}

	return fmtstr(res.ETag), nil
}

func (bs *OCIBlobstore) multipartUpload(ctx context.Context, expectedVersion, key string, numParts int, uploadSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)

	startReq := objectstorage.CreateMultipartUploadRequest{
		NamespaceName: &bs.namespace,
		BucketName:    &bs.bucketName,
		CreateMultipartUploadDetails: objectstorage.CreateMultipartUploadDetails{
			Object: &absKey,
		},
	}

	star := "*"
	if expectedVersion != "" {
		startReq.IfMatch = &expectedVersion
	} else {
		startReq.IfNoneMatch = &star
	}

	startRes, err := bs.client.CreateMultipartUpload(ctx, startReq)
	if err != nil {
		return "", err
	}

	parts, err := bs.uploadParts(ctx, absKey, fmtstr(startRes.UploadId), numParts, uploadSize, reader)
	if err != nil {
		// ignore this error
		bs.client.AbortMultipartUpload(ctx, objectstorage.AbortMultipartUploadRequest{
			NamespaceName:   &bs.namespace,
			BucketName:      &bs.bucketName,
			ObjectName:      &absKey,
			UploadId:        startRes.UploadId,
			RequestMetadata: common.RequestMetadata{},
		})
		return "", err
	}

	commitReq := objectstorage.CommitMultipartUploadRequest{
		NamespaceName:                &bs.namespace,
		BucketName:                   &bs.bucketName,
		ObjectName:                   &absKey,
		UploadId:                     startRes.UploadId,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: parts},
	}

	if expectedVersion != "" {
		commitReq.IfMatch = &expectedVersion
	} else {
		commitReq.IfNoneMatch = &star
	}

	commitRes, err := bs.client.CommitMultipartUpload(ctx, commitReq)
	if err != nil {
		return "", err
	}

	return fmtstr(commitRes.ETag), nil
}

func (bs *OCIBlobstore) uploadParts(ctx context.Context, objectName, uploadID string, numParts int, totalSize int64, reader io.Reader) ([]objectstorage.CommitMultipartUploadPartDetails, error) {
	return uploadParts(ctx, objectName, uploadID, numParts, bs.concurrentListeners, totalSize, defaultBatchSize, reader, bs.uploadPart)
}

func (bs *OCIBlobstore) uploadPart(ctx context.Context, objectName, uploadID string, partNumber int, contentLength int64, reader io.Reader) (objectstorage.CommitMultipartUploadPartDetails, error) {
	if objectName == "" {
		return objectstorage.CommitMultipartUploadPartDetails{}, errors.New("object name required to upload part")
	}

	if uploadID == "" {
		return objectstorage.CommitMultipartUploadPartDetails{}, errors.New("upload id required to upload part")
	}

	res, err := bs.client.UploadPart(ctx, objectstorage.UploadPartRequest{
		NamespaceName:  &bs.namespace,
		BucketName:     &bs.bucketName,
		ObjectName:     &objectName,
		UploadId:       &uploadID,
		UploadPartNum:  &partNumber,
		ContentLength:  &contentLength,
		UploadPartBody: io.NopCloser(reader),
	})
	if err != nil {
		return objectstorage.CommitMultipartUploadPartDetails{}, err
	}

	return objectstorage.CommitMultipartUploadPartDetails{
		Etag:    res.ETag,
		PartNum: &partNumber,
	}, nil
}

func uploadParts(ctx context.Context, objectName, uploadID string, numParts, concurrentListeners int, totalSize, maxBatchSize int64, reader io.Reader, uploadF uploadFunc) ([]objectstorage.CommitMultipartUploadPartDetails, error) {
	completedParts := make([]objectstorage.CommitMultipartUploadPartDetails, numParts)
	partSize := int64(math.Ceil(float64(totalSize) / float64(numParts)))

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrentListeners)

	batch := make([]*toUpload, 0)
	batchSize := int64(0)
	partNum := 1

	for {
		if batchSize >= maxBatchSize {
			for _, u := range batch {
				eg.Go(func() error {
					cp, err := uploadF(egCtx, objectName, uploadID, u.partNum, int64(len(u.b)), bytes.NewReader(u.b))
					if err != nil {
						return err
					}
					completedParts[u.partNum-1] = cp
					return nil
				})
			}

			batchSize = 0
			batch = make([]*toUpload, 0)
			continue
		}

		buf := make([]byte, partSize)
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		buf = buf[:n]
		batchSize += int64(n)
		batch = append(batch, &toUpload{
			b:       buf,
			partNum: partNum,
		})

		partNum++
	}

	if batchSize > 0 && len(batch) > 0 {
		for _, u := range batch {
			eg.Go(func() error {
				cp, err := uploadF(egCtx, objectName, uploadID, u.partNum, int64(len(u.b)), bytes.NewReader(u.b))
				if err != nil {
					return err
				}
				completedParts[u.partNum-1] = cp
				return nil
			})
		}
	}

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return completedParts, nil
}

func getNumPartsAndPartSize(totalSize, partSize, maxPartNum int64) (int, int64) {
	ps := int64(math.Ceil(float64(totalSize) / float64(maxPartNum)))
	if ps < partSize {
		numParts := int(math.Ceil(float64(totalSize) / float64(partSize)))
		return numParts, partSize
	}
	numParts := int(math.Ceil(float64(totalSize) / float64(ps)))
	return numParts, ps
}

func fmtstr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
