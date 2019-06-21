package blobstore

import (
	"context"
	"io"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

const (
	precondFailCode = 412
)

// GCSBlobstore provides a GCS implementation of the Blobstore interface
type GCSBlobstore struct {
	bucket *storage.BucketHandle
	prefix string
}

// NewGCSBlobstore creates a new instance of a GCSBlobstare
func NewGCSBlobstore(bucket *storage.BucketHandle, prefix string) *GCSBlobstore {
	return &GCSBlobstore{bucket, prefix}
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *GCSBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	oh := bs.bucket.Object(bs.prefix + key)
	_, err := oh.Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		return false, nil
	}

	return err == nil, err
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *GCSBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	oh := bs.bucket.Object(bs.prefix + key)
	attrs, err := oh.Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		return nil, "", NotFound{key}
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
	absKey := bs.prefix + key
	oh := bs.bucket.Object(absKey)
	writer := oh.NewWriter(ctx)

	return writeObj(writer, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *GCSBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	oh := bs.bucket.Object(bs.prefix + key)

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
