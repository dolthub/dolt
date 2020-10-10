// Copyright 2019 Liquidata, Inc.
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
	"io"
	"path"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

const (
	precondFailCode = 412
)

// GCSBlobstore provides a GCS implementation of the Blobstore interface
type GCSBlobstore struct {
	bucket     *storage.BucketHandle
	bucketName string
	prefix     string
}

// NewGCSBlobstore creates a new instance of a GCSBlobstare
func NewGCSBlobstore(gcs *storage.Client, bucketName, prefix string) *GCSBlobstore {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	bucket := gcs.Bucket(bucketName)
	return &GCSBlobstore{bucket, bucketName, prefix}
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *GCSBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := path.Join(bs.prefix, key)
	oh := bs.bucket.Object(absKey)
	_, err := oh.Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		return false, nil
	}

	return err == nil, err
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *GCSBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	absKey := path.Join(bs.prefix, key)
	oh := bs.bucket.Object(absKey)
	attrs, err := oh.Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		return nil, "", NotFound{"gs://" + path.Join(bs.bucketName, absKey)}
	} else if err != nil {
		return nil, "", err
	}

	generation := attrs.Generation

	var reader *storage.Reader
	if br.isAllRange() {
		reader, err = oh.Generation(generation).NewReader(ctx)
	} else {
		posBr := br.positiveRange(attrs.Size)
		reader, err = oh.Generation(generation).NewRangeReader(ctx, posBr.offset, posBr.length)
	}

	if err != nil {
		return nil, "", err
	}

	return reader, strconv.FormatInt(generation, 16), nil
}

func writeObj(writer *storage.Writer, reader io.Reader) (string, error) {
	writeErr, closeErr := func() (writeErr error, closeErr error) {
		defer func() {
			closeErr = writer.Close()
		}()
		_, writeErr = io.Copy(writer, reader)

		return
	}()

	if writeErr != nil {
		return "", writeErr
	} else if closeErr != nil {
		return "", closeErr
	}

	generation := writer.Attrs().Generation

	return strconv.FormatInt(generation, 16), nil
}

// Put sets the blob and the version for a key
func (bs *GCSBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	oh := bs.bucket.Object(absKey)
	writer := oh.NewWriter(ctx)

	return writeObj(writer, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *GCSBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	oh := bs.bucket.Object(absKey)

	var conditionalHandle *storage.ObjectHandle
	if expectedVersion != "" {
		expectedGen, err := strconv.ParseInt(expectedVersion, 16, 64)

		if err != nil {
			panic("Invalid expected Version")
		}

		conditionalHandle = oh.If(storage.Conditions{GenerationMatch: expectedGen})
	} else {
		conditionalHandle = oh.If(storage.Conditions{DoesNotExist: true})
	}

	writer := conditionalHandle.NewWriter(ctx)

	ver, err := writeObj(writer, reader)

	if err != nil {
		apiErr, ok := err.(*googleapi.Error)

		if ok {
			if apiErr.Code == precondFailCode {
				return "", CheckAndPutError{key, expectedVersion, "unknown (Not supported in GCS implementation)"}
			}
		}
	}

	return ver, err
}
