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
	"io"
	"strconv"
	"strings"
)

// Blobstore is an interface for storing and retrieving blobs of data by key
type Blobstore interface {
	// Path returns this blobstore's path.
	Path() (path string)

	// Exists returns true if a blob keyed by |key| exists.
	Exists(ctx context.Context, key string) (ok bool, err error)

	// Get returns a byte range of from the blob keyed by |key|, and the latest store version.
	Get(ctx context.Context, key string, br BlobRange) (rc io.ReadCloser, size uint64, version string, err error)

	// Put creates a new blob from |reader| keyed by |key|, it returns the latest store version.
	Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (version string, err error)

	// CheckAndPut updates the blob keyed by |key| using a check-and-set on |expectedVersion|.
	CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (version string, err error)

	// Concatenate creates a new blob named |key| by concatenating |sources|.
	Concatenate(ctx context.Context, key string, sources []string) (version string, err error)
}

// GetBytes is a utility method calls bs.Get and handles reading the data from the returned
// io.ReadCloser and closing it.
func GetBytes(ctx context.Context, bs Blobstore, key string, br BlobRange) ([]byte, string, error) {
	rc, _, ver, err := bs.Get(ctx, key, br)

	if err != nil || rc == nil {
		return nil, ver, err
	}

	defer rc.Close()
	data, err := io.ReadAll(rc)

	if err != nil {
		return nil, "", err
	}

	return data, ver, nil
}

// PutBytes is a utility method calls bs.Put by wrapping the supplied []byte in an io.Reader
func PutBytes(ctx context.Context, bs Blobstore, key string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	return bs.Put(ctx, key, int64(len(data)), reader)
}

// parseContentRangeSize extracts the total size from a Content-Range header.
// Expected format: "bytes start-end/total" e.g., "bytes 0-1023/1234567"
// Returns 0 if the header is malformed or not present.
func parseContentRangeSize(contentRange string) uint64 {
	if contentRange == "" {
		return 0
	}
	i := strings.Index(contentRange, "/")
	if i == -1 {
		return 0
	}
	sizeStr := contentRange[i+1:]
	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return 0
	}
	return size
}
