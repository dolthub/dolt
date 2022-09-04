package blobstore

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"io"
	"net/http"
	"path"
	"strconv"
)

const (
	enabled = "Enabled"
)

// OSSBlobstore provides an Aliyun OSS implementation of the Blobstore interface
type OSSBlobstore struct {
	bucket        *oss.Bucket
	bucketName    string
	enableVersion bool
	prefix        string
}

// NewOSSBlobstore creates a new instance of a OSSBlobstore
func NewOSSBlobstore(ossClient *oss.Client, bucketName, prefix string) (*OSSBlobstore, error) {
	prefix = normalizePrefix(prefix)
	bucket, err := ossClient.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	// check if bucket enable versioning
	versionStatus, err := ossClient.GetBucketVersioning(bucketName)
	if err != nil {
		return nil, err
	}
	return &OSSBlobstore{
		bucket:        bucket,
		bucketName:    bucketName,
		prefix:        prefix,
		enableVersion: versionStatus.Status == enabled,
	}, nil
}

func (ob *OSSBlobstore) Exists(_ context.Context, key string) (bool, error) {
	return ob.bucket.IsObjectExist(ob.absKey(key))
}

func (ob *OSSBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	absKey := ob.absKey(key)
	meta, err := ob.bucket.GetObjectMeta(absKey)

	if isNotFoundErr(err) {
		return nil, "", NotFound{"oss://" + path.Join(ob.bucketName, absKey)}
	}

	if br.isAllRange() {
		reader, err := ob.bucket.GetObject(absKey)
		if err != nil {
			return nil, "", err
		}
		return reader, "", nil
	}
	size, err := strconv.ParseInt(meta.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return nil, "", err
	}
	posBr := br.positiveRange(size)
	reader, err := ob.bucket.GetObject(absKey, oss.Range(posBr.offset, posBr.offset+posBr.length-1))
	if err != nil {
		return nil, "", err
	}
	return reader, oss.GetVersionId(meta), nil
}

func (ob *OSSBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	var meta http.Header
	if err := ob.bucket.PutObject(ob.absKey(key), reader, oss.GetResponseHeader(&meta)); err != nil {
		return "", err
	}
	return oss.GetVersionId(meta), nil
}

func (ob *OSSBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	var options []oss.Option
	if expectedVersion != "" {
		options = append(options, oss.VersionId(expectedVersion))
	} else {
		options = append(options, oss.ForbidOverWrite(true))
	}
	var meta http.Header
	if err := ob.bucket.PutObject(ob.absKey(key), reader, oss.GetResponseHeader(&meta)); err != nil {
		return "", err
	}
	return oss.GetVersionId(meta), nil
}

func (ob *OSSBlobstore) absKey(key string) string {
	return path.Join(ob.prefix, key)
}

func normalizePrefix(prefix string) string {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return prefix
}

func isNotFoundErr(err error) bool {
	switch err.(type) {
	case oss.ServiceError:
		if err.(oss.ServiceError).StatusCode == 404 {
			return true
		}
	}
	return false
}
