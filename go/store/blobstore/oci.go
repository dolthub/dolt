// Copyright 2019 Dolthub, Inc.
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
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"path"
	"sort"
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

// OCIBlobstore provides an OCI implementation of the Blobstore interface
type OCIBlobstore struct {
	provider            common.ConfigurationProvider
	client              objectstorage.ObjectStorageClient
	bucketName          string
	nameSpace           string
	prefix              string
	concurrentListeners int
}

var _ Blobstore = &OCIBlobstore{}

// NewOCIBlobstore creates a new instance of a OCIBlobstore
func NewOCIBlobstore(provider common.ConfigurationProvider, client objectstorage.ObjectStorageClient, bucketName, prefix string) *OCIBlobstore {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return &OCIBlobstore{provider, client, bucketName, "", prefix, defaultConcurrentListeners}
}

func (bs *OCIBlobstore) Path() string {
	return path.Join(bs.bucketName, bs.prefix)
}

func (bs *OCIBlobstore) getNamespace(ctx context.Context) (string, error) {
	if bs.nameSpace != "" {
		return bs.nameSpace, nil
	}
	request := objectstorage.GetNamespaceRequest{}
	r, err := bs.client.GetNamespace(ctx, request)
	if err != nil {
		return "", err
	}
	bs.nameSpace = *r.Value
	return bs.nameSpace, nil
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *OCIBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := path.Join(bs.prefix, key)
	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return false, err
	}
	_, err = bs.client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
	})
	if err == nil {
		return true, nil
	}
	return false, nil
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *OCIBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	absKey := path.Join(bs.prefix, key)
	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return nil, "", err
	}

	req := objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
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
				return nil, "", NotFound{"oci://" + path.Join(bs.bucketName, absKey)}
			}
		}
		return nil, "", err
	}

	// handle negative offset and positive length
	if br.offset < 0 && br.length > 0 {
		trimmedR, err := trimContent(res, br.length)
		if err != nil {
			return nil, "", err
		}
		return trimmedR, fmtstr(res.ETag), nil
	}

	return res.Content, fmtstr(res.ETag), nil
}

// Put sets the blob and the version for a key
func (bs *OCIBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	return bs.upload(ctx, "", key, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *OCIBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	return bs.upload(ctx, expectedVersion, key, reader)
}

// At the time of this implementation, Oracle Cloud does not provide a way to create composite objects
// via their APIs/SDKs.
func (bs *OCIBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	panic("concatenate is unimplemented on the oci blobstore")
}

func (bs *OCIBlobstore) upload(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	numParts, totalSize, r, err := getUploadInfo(defaultPartSize, maxPartNum, reader)
	if err != nil {
		return "", err
	}

	if totalSize == 0 {
		return "", errors.New("failed to upload to oci blobstore, no data in reader")
	} else if totalSize < minPartSize {
		return bs.checkAndPut(ctx, expectedVersion, key, totalSize, r)
	} else {
		return bs.multipartUpload(ctx, expectedVersion, key, numParts, totalSize, r)
	}
}

func (bs *OCIBlobstore) checkAndPut(ctx context.Context, expectedVersion, key string, contentLength int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return "", err
	}

	req := objectstorage.PutObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
		ContentLength: &contentLength,
		PutObjectBody: io.NopCloser(reader),
	}

	if expectedVersion != "" {
		req.IfMatch = &expectedVersion
	}

	res, err := bs.client.PutObject(ctx, req)
	if err != nil {
		return "", CheckAndPutError{key, expectedVersion, "unknown (Not supported in OCI implementation)"}
	}

	return fmtstr(res.ETag), nil
}

func (bs *OCIBlobstore) multipartUpload(ctx context.Context, expectedVersion, key string, numParts int, uploadSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return "", err
	}

	startReq := objectstorage.CreateMultipartUploadRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		CreateMultipartUploadDetails: objectstorage.CreateMultipartUploadDetails{
			Object: &absKey,
		},
	}

	if expectedVersion != "" {
		startReq.IfMatch = &expectedVersion
	}

	startRes, err := bs.client.CreateMultipartUpload(ctx, startReq)
	if err != nil {
		return "", err
	}

	parts, err := bs.uploadParts(ctx, absKey, fmtstr(startRes.UploadId), numParts, uploadSize, reader)
	if err != nil {
		// ignore this error
		bs.client.AbortMultipartUpload(ctx, objectstorage.AbortMultipartUploadRequest{
			NamespaceName:   &namespace,
			BucketName:      &bs.bucketName,
			ObjectName:      &absKey,
			UploadId:        startRes.UploadId,
			RequestMetadata: common.RequestMetadata{},
		})
		return "", err
	}

	commitReq := objectstorage.CommitMultipartUploadRequest{
		NamespaceName:                &namespace,
		BucketName:                   &bs.bucketName,
		ObjectName:                   &absKey,
		UploadId:                     startRes.UploadId,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: parts},
	}

	if expectedVersion != "" {
		commitReq.IfMatch = &expectedVersion
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

	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return objectstorage.CommitMultipartUploadPartDetails{}, err
	}

	res, err := bs.client.UploadPart(ctx, objectstorage.UploadPartRequest{
		NamespaceName:  &namespace,
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
	completedPartChan := make(chan objectstorage.CommitMultipartUploadPartDetails, numParts)
	completedParts := make([]objectstorage.CommitMultipartUploadPartDetails, numParts)
	partSize := int64(math.Ceil(float64(totalSize) / float64(numParts)))

	batch := make([]*toUpload, 0)
	batchSize := int64(0)
	partNum := 1
	for {
		if batchSize >= maxBatchSize {
			err := uploadBatch(ctx, objectName, uploadID, batch, completedPartChan, concurrentListeners, uploadF)
			if err != nil {
				close(completedPartChan)
				return nil, err
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
			close(completedPartChan)
			return nil, err
		}

		batchSize += int64(n)
		batch = append(batch, &toUpload{
			b:       buf,
			partNum: partNum,
		})

		partNum++
	}

	if batchSize > 0 && len(batch) > 0 {
		err := uploadBatch(ctx, objectName, uploadID, batch, completedPartChan, concurrentListeners, uploadF)
		if err != nil {
			close(completedPartChan)
			return nil, err
		}
	}

	close(completedPartChan)

	idx := 0
	for cp := range completedPartChan {
		completedParts[idx] = cp
		idx++
	}

	sort.Slice(completedParts, func(i, j int) bool {
		if completedParts[i].PartNum == nil || completedParts[j].PartNum == nil {
			return false
		}
		return *completedParts[i].PartNum < *completedParts[j].PartNum
	})

	return completedParts, nil
}

func uploadBatch(ctx context.Context, objectName, uploadID string, batch []*toUpload, completedPartsChan chan objectstorage.CommitMultipartUploadPartDetails, concurrentListeners int, uploadF uploadFunc) error {
	batchChan := make(chan *toUpload, len(batch))
	eg, egCtx := errgroup.WithContext(ctx)

	for i := 0; i < concurrentListeners; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return nil
				case u, ok := <-batchChan:
					if !ok {
						return nil
					}

					cp, err := uploadF(egCtx, objectName, uploadID, u.partNum, int64(len(u.b)), bytes.NewReader(u.b))
					if err != nil {
						return err
					}

					completedPartsChan <- cp
				}
			}
		})
	}

	eg.Go(func() error {
		defer close(batchChan)

		for {
			select {
			case <-egCtx.Done():
				return nil
			default:
				for _, u := range batch {
					batchChan <- u
				}
				return nil
			}
		}
	})

	return eg.Wait()
}

func getUploadInfo(partSize, maxPartNum int, r io.Reader) (int, int64, io.Reader, error) {
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)

	totalSize := int64(0)
	for {
		b := make([]byte, partSize)

		n, err := tee.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, 0, nil, err
		}
		totalSize += int64(n)
	}

	ps := int64(math.Ceil(float64(totalSize) / float64(maxPartNum)))

	if ps < int64(partSize) {
		numParts := int(math.Ceil(float64(totalSize) / float64(partSize)))
		return numParts, totalSize, &buf, nil
	}

	numParts := int(math.Ceil(float64(totalSize) / float64(ps)))
	return numParts, totalSize, &buf, nil
}

func trimContent(res objectstorage.GetObjectResponse, length int64) (io.ReadCloser, error) {
	defer res.Content.Close()
	var data bytes.Buffer
	lr := io.LimitReader(res.Content, length)
	n, err := io.Copy(&data, lr)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, errors.New("failed to trim response content")
	}
	return io.NopCloser(&data), nil
}

func fmtstr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
