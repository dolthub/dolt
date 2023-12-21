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
	"io"
	"math"
	"os"
	"path"
	"sort"

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

type toDownload struct {
	path    string
	partNum int
	br      BlobRange
}

type uploadFunc func(ctx context.Context, objectName, uploadID string, partNumber int, contentLength int64, reader io.Reader) (objectstorage.CommitMultipartUploadPartDetails, error)

type downloadFunc func(ctx context.Context, objectName, namespace, etag string, partNumber int, br BlobRange) (*toDownload, error)

type tempLocalObject struct {
	path string
	f    *os.File
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
	_, err = bs.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
		NamespaceName: &namespace,
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
func (bs *OCIBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	absKey := path.Join(bs.prefix, key)
	namespace, err := bs.getNamespace(ctx)
	if err != nil {
		return nil, "", err
	}

	req := objectstorage.HeadObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
	}

	res, err := bs.client.HeadObject(ctx, req)
	if err != nil {
		if serr, ok := common.IsServiceError(err); ok {
			// handle not found code
			if serr.GetHTTPStatusCode() == 404 {
				return nil, "", NotFound{"oci://" + path.Join(bs.bucketName, absKey)}
			}
		}
		return nil, "", err
	}

	requestedSize, totalSize, offSet, err := getDownloadInfo(br, res.ContentLength)
	if err != nil {
		return nil, "", NotFound{"oci://" + path.Join(bs.bucketName, absKey)}
	}

	etag := fmtstr(res.ETag)

	return bs.multipartDownload(ctx, absKey, namespace, etag, requestedSize, totalSize, offSet)
}

func (bs *OCIBlobstore) multipartDownload(ctx context.Context, absKey, namespace, etag string, requestedSize, totalSize, offSet int64) (io.ReadCloser, string, error) {
	parts, err := downloadParts(ctx, absKey, namespace, etag, requestedSize, totalSize, defaultBatchSize, offSet, defaultPartSize, maxPartNum, defaultConcurrentListeners, bs.downloadPart)
	if err != nil {
		return nil, "", err
	}
	rc, err := assembleParts(parts)
	return rc, etag, err
}

func (bs *OCIBlobstore) getObject(ctx context.Context, absKey, namespace, etag string, br BlobRange) (io.ReadCloser, string, error) {
	req := objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &absKey,
	}

	byteRange := br.asHttpRangeHeader()
	if byteRange != "" {
		req.Range = &byteRange
	}

	if etag != "" {
		req.IfMatch = &etag
	} else {
		star := "*"
		req.IfNoneMatch = &star
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
	panic("concatenate is unimplemented on the oci blobstore")
}

type Lenner interface {
	Len() int
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

func (bs *OCIBlobstore) downloadPart(ctx context.Context, objectName, namespace, etag string, partNumber int, br BlobRange) (*toDownload, error) {
	if objectName == "" {
		return nil, errors.New("object name required to upload part")
	}

	req := objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bs.bucketName,
		ObjectName:    &objectName,
	}

	star := "*"
	if etag != "" {
		req.IfMatch = &etag
	} else {
		req.IfNoneMatch = &star
	}

	byteRange := br.asHttpRangeHeader()
	if byteRange != "" {
		req.Range = &byteRange
	}

	res, err := bs.client.GetObject(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Content.Close()

	p, err := createTempFile()
	if err != nil {
		return nil, err
	}

	w, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer w.Close()

	_, err = io.Copy(w, res.Content)
	if err != nil {
		return nil, err
	}

	return &toDownload{
		path:    p,
		partNum: partNumber,
		br:      br,
	}, nil
}

func createTempFile() (string, error) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		return "", err
	}
	defer f.Close()
	return f.Name(), nil
}

func copyFileToWriter(path string, w io.Writer) error {
	r, err := os.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(w, r)
	return err
}

func appendPartsToFile(path string, completedParts []*toDownload) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, p := range completedParts {
		err = copyFileToWriter(p.path, f)
		if err != nil {
			return err
		}

		err = os.Remove(p.path)
		if err != nil {
			return err
		}
	}

	return nil
}

func assembleParts(completedParts []*toDownload) (io.ReadCloser, error) {
	p, err := createTempFile()
	if err != nil {
		return nil, err
	}

	err = appendPartsToFile(p, completedParts)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	return &tempLocalObject{
		path: p,
		f:    f,
	}, nil
}

func downloadParts(ctx context.Context, objectName, namespace, etag string, requestedSize, totalSize, maxBatchSize, offSet, minPartSize, maxPartNum int64, concurrentListeners int, downloadF downloadFunc) ([]*toDownload, error) {
	numParts, partSize := getNumPartsAndPartSize(requestedSize, minPartSize, maxPartNum)
	completedPartChan := make(chan *toDownload, numParts)
	completedParts := make([]*toDownload, numParts)

	batch := make([]*toDownload, 0)
	batchSize := int64(0)
	partNum := 1
	currentOffSet := offSet

	for partNum <= numParts {
		if batchSize >= maxBatchSize {
			err := downloadBatch(ctx, objectName, namespace, etag, batch, completedPartChan, concurrentListeners, downloadF)
			if err != nil {
				close(completedPartChan)
				return nil, err
			}

			batchSize = 0
			batch = make([]*toDownload, 0)
			continue
		}

		batch = append(batch, &toDownload{
			partNum: partNum,
			br:      NewBlobRange(currentOffSet, partSize),
		})

		batchSize += partSize
		partNum++
		currentOffSet = int64(math.Min(float64(partSize)+float64(currentOffSet), float64(totalSize)-float64(1)))
	}

	if batchSize > 0 && len(batch) > 0 {
		err := downloadBatch(ctx, objectName, namespace, etag, batch, completedPartChan, concurrentListeners, downloadF)
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
		return completedParts[i].partNum < completedParts[j].partNum
	})

	return completedParts, nil
}

func downloadBatch(ctx context.Context, objectName, namespace, etag string, batch []*toDownload, completedPartsChan chan *toDownload, concurrentListeners int, downloadF downloadFunc) error {
	batchChan := make(chan *toDownload, len(batch))
	eg, egCtx := errgroup.WithContext(ctx)

	for i := 0; i < concurrentListeners; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return nil
				case d, ok := <-batchChan:
					if !ok {
						return nil
					}

					cp, err := downloadF(egCtx, objectName, namespace, etag, d.partNum, d.br)
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
				for _, d := range batch {
					batchChan <- d
				}
				return nil
			}
		}
	})

	return eg.Wait()
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

		buf = buf[:n]
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

func getDownloadInfo(br BlobRange, contentLength *int64) (int64, int64, int64, error) {
	totalSize := int64(0)
	if contentLength != nil {
		totalSize = *contentLength
	}

	if totalSize == 0 {
		return 0, 0, 0, errors.New("no content to download")
	}

	offSet := int64(0)
	requestedSize := int64(0)

	if br.offset >= totalSize {
		offSet = totalSize - 1
		requestedSize = 0
	} else if br.isAllRange() {
		requestedSize = totalSize
	} else if br.length == 0 && br.offset != 0 {
		if br.offset < 0 {
			requestedSize = int64(math.Abs(float64(br.offset)))
			offSet = totalSize + br.offset
		} else {
			requestedSize = totalSize - br.offset
			offSet = br.offset
		}
	} else {
		requestedSize = br.length
		if br.offset < 0 {
			offSet = totalSize + br.offset
		} else {
			offSet = br.offset
		}
	}

	return requestedSize, totalSize, offSet, nil
}

//func getUploadInfo(partSize, maxPartNum int, r io.Reader) (int, int64, io.Reader, error) {
//	var buf bytes.Buffer
//	tee := io.TeeReader(r, &buf)
//
//	totalSize := int64(0)
//	for {
//		b := make([]byte, partSize)
//
//		n, err := tee.Read(b)
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			return 0, 0, nil, err
//		}
//		totalSize += int64(n)
//	}
//
//	numParts, _ := getNumPartsAndPartSize(totalSize, int64(partSize), int64(maxPartNum))
//	return numParts, totalSize, &buf, nil
//}

func getNumPartsAndPartSize(totalSize, partSize, maxPartNum int64) (int, int64) {
	ps := int64(math.Ceil(float64(totalSize) / float64(maxPartNum)))
	if ps < partSize {
		numParts := int(math.Ceil(float64(totalSize) / float64(partSize)))
		return numParts, partSize
	}
	numParts := int(math.Ceil(float64(totalSize) / float64(ps)))
	return numParts, ps
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
