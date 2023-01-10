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
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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

var _ Blobstore = &OSSBlobstore{}

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

func (ob *OSSBlobstore) Path() string {
	return path.Join(ob.bucketName, ob.prefix)
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
		return reader, ob.getVersion(meta), nil
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
	return reader, ob.getVersion(meta), nil
}

func (ob *OSSBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	var meta http.Header
	if err := ob.bucket.PutObject(ob.absKey(key), reader, oss.GetResponseHeader(&meta)); err != nil {
		return "", err
	}
	return ob.getVersion(meta), nil
}

func (ob *OSSBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	var options []oss.Option
	if expectedVersion != "" {
		options = append(options, oss.VersionId(expectedVersion))
	}
	var meta http.Header
	options = append(options, oss.GetResponseHeader(&meta))
	if err := ob.bucket.PutObject(ob.absKey(key), reader, options...); err != nil {
		ossErr, ok := err.(oss.ServiceError)
		if ok {
			return "", CheckAndPutError{
				Key:             key,
				ExpectedVersion: expectedVersion,
				ActualVersion:   fmt.Sprintf("unknown (OSS error code %d)", ossErr.StatusCode)}
		}
		return "", err
	}
	return ob.getVersion(meta), nil
}

func (ob *OSSBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return "", fmt.Errorf("Conjoin is not implemented for OSSBlobstore")
}

func (ob *OSSBlobstore) absKey(key string) string {
	return path.Join(ob.prefix, key)
}

func (ob *OSSBlobstore) getVersion(meta http.Header) string {
	if ob.enableVersion {
		return oss.GetVersionId(meta)
	}
	return ""
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
