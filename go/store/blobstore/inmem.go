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
	"fmt"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
)

type byteSliceReadCloser struct {
	io.Reader
	io.Closer
}

func newByteSliceReadCloser(data []byte) *byteSliceReadCloser {
	reader := bytes.NewReader(data)
	return &byteSliceReadCloser{reader, io.NopCloser(reader)}
}

// InMemoryBlobstore provides an in memory implementation of the Blobstore interface
type InMemoryBlobstore struct {
	path     string
	mutex    sync.RWMutex
	blobs    map[string][]byte
	versions map[string]string
}

var _ Blobstore = &InMemoryBlobstore{}

// NewInMemoryBlobstore creates an instance of an InMemoryBlobstore
func NewInMemoryBlobstore(path string) *InMemoryBlobstore {
	return &InMemoryBlobstore{
		path:     path,
		blobs:    make(map[string][]byte),
		versions: make(map[string]string),
	}
}

func (bs *InMemoryBlobstore) Path() string {
	return bs.path
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *InMemoryBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()

	if val, ok := bs.blobs[key]; ok {
		if ver, ok := bs.versions[key]; ok && ver != "" {
			var byteRange []byte
			if br.isAllRange() {
				byteRange = val
			} else {
				posBR := br.positiveRange(int64(len(val)))
				if posBR.length == 0 {
					byteRange = val[posBR.offset:]
				} else {
					byteRange = val[posBR.offset : posBR.offset+posBR.length]
				}
			}

			return newByteSliceReadCloser(byteRange), ver, nil
		}

		panic("Blob without version, or with invalid version, should no be possible.")
	}

	return nil, "", NotFound{key}
}

// Put sets the blob and the version for a key
func (bs *InMemoryBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	return bs.put(ctx, key, reader)
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *InMemoryBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	ver, ok := bs.versions[key]
	check := !ok && expectedVersion == "" || ok && expectedVersion == ver

	if !check {
		return "", CheckAndPutError{key, expectedVersion, ver}
	}
	return bs.put(ctx, key, reader)
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *InMemoryBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	_, ok := bs.blobs[key]
	return ok, nil
}

func (bs *InMemoryBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	// recursively compose sources (mirrors GCS impl)
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
		// execute compose calls concurrently (mirrors GCS impl)
		eg, _ := errgroup.WithContext(ctx)
		for i := 0; i < len(batches); i++ {
			idx := i
			eg.Go(func() error {
				blob, err := bs.composeObjects(batches[idx])
				if err != nil {
					return err
				}
				_, err = bs.Put(ctx, next[idx], int64(len(blob)), bytes.NewReader(blob))
				return err
			})
		}
		if err := eg.Wait(); err != nil {
			return "", err
		}
		sources = next
	}

	blob, err := bs.composeObjects(sources)
	if err != nil {
		return "", err
	}
	return bs.Put(ctx, key, int64(len(blob)), bytes.NewReader(blob))
}

func (bs *InMemoryBlobstore) put(ctx context.Context, key string, reader io.Reader) (string, error) {
	ver := uuid.New().String()
	data, err := io.ReadAll(reader)

	if err != nil {
		return "", err
	}

	bs.blobs[key] = data
	bs.versions[key] = ver

	return ver, nil
}

func (bs *InMemoryBlobstore) composeObjects(sources []string) (blob []byte, err error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	if len(sources) > composeBatch {
		return nil, fmt.Errorf("too many objects to compose (%d > %d)", len(sources), composeBatch)
	}
	var sz int
	for _, k := range sources {
		sz += len(bs.blobs[k])
	}
	blob = make([]byte, 0, sz)
	for _, k := range sources {
		blob = append(blob, bs.blobs[k]...)
	}
	return
}
