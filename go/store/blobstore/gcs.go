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
	"path"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
)

const (
	precondFailCode = 412

	composeBatch = 32
)

// GCSBlobstore provides a GCS implementation of the Blobstore interface
type GCSBlobstore struct {
	bucket     *storage.BucketHandle
	bucketName string
	prefix     string
}

var _ Blobstore = &GCSBlobstore{}

// NewGCSBlobstore creates a new instance of a GCSBlobstore
func NewGCSBlobstore(gcs *storage.Client, bucketName, prefix string) *GCSBlobstore {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	bucket := gcs.Bucket(bucketName)
	return &GCSBlobstore{bucket, bucketName, prefix}
}

func (bs *GCSBlobstore) Path() string {
	return path.Join(bs.bucketName, bs.prefix)
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
	var reader *storage.Reader
	var err error
	if br.isAllRange() {
		reader, err = oh.NewReader(ctx)
	} else {
		offset, length := br.offset, br.length
		if offset < 0 {
			length = -1
		}
		reader, err = oh.NewRangeReader(ctx, offset, length)
	}

	if err == storage.ErrObjectNotExist {
		return nil, "", NotFound{"gs://" + path.Join(bs.bucketName, absKey)}
	} else if err != nil {
		return nil, "", err
	}

	attrs := reader.Attrs
	generation := attrs.Generation

	return reader, fmtGeneration(generation), nil
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

	return fmtGeneration(generation), nil
}

// Put sets the blob and the version for a key
func (bs *GCSBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)
	oh := bs.bucket.Object(absKey)
	writer := oh.NewWriter(ctx)

	return writeObj(writer, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *GCSBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
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

func (bs *GCSBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	// GCS compose has a batch size limit,
	// recursively compose sources
	for len(sources) > composeBatch {
		// compose subsets of |sources| in batches,
		// store tmp composite objects in |next|
		var next []string
		var batches [][]string
		for len(sources) > 0 {
			k := min(composeBatch, len(sources))
			batches = append(batches, sources[:k])
			next = append(next, uuid.New().String())
			sources = sources[k:]
		}
		// execute compose calls concurrently
		eg, ectx := errgroup.WithContext(ctx)
		for i := 0; i < len(batches); i++ {
			idx := i
			eg.Go(func() (err error) {
				_, err = bs.composeObjects(ectx, next[idx], batches[idx])
				return
			})
		}
		if err := eg.Wait(); err != nil {
			return "", err
		}
		sources = next
	}
	return bs.composeObjects(ctx, key, sources)
}

func (bs *GCSBlobstore) composeObjects(ctx context.Context, composite string, sources []string) (gen string, err error) {
	if len(sources) > composeBatch {
		return "", fmt.Errorf("too many objects to compose (%d > %d)", len(sources), composeBatch)
	}

	objects := make([]*storage.ObjectHandle, len(sources))
	eg, ectx := errgroup.WithContext(ctx)
	for i := range objects {
		idx := i
		eg.Go(func() (err error) {
			var a *storage.ObjectAttrs
			oh := bs.bucket.Object(path.Join(bs.prefix, sources[idx]))
			if a, err = oh.Attrs(ectx); err != nil {
				return err
			}
			objects[idx] = oh.Generation(a.Generation)
			return
		})
	}
	if err = eg.Wait(); err != nil {
		return "", err
	}

	// compose |objects| into |c|
	var a *storage.ObjectAttrs
	c := bs.bucket.Object(path.Join(bs.prefix, composite))
	if a, err = c.ComposerFrom(objects...).Run(ctx); err != nil {
		return "", err
	}
	return fmtGeneration(a.Generation), nil
}

func fmtGeneration(g int64) string {
	return strconv.FormatInt(g, 16)
}
