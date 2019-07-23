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
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sync"

	"github.com/google/uuid"
)

type byteSliceReadCloser struct {
	io.Reader
	io.Closer
}

func newByteSliceReadCloser(data []byte) *byteSliceReadCloser {
	reader := bytes.NewReader(data)
	return &byteSliceReadCloser{reader, ioutil.NopCloser(reader)}
}

// InMemoryBlobstore provides an in memory implementation of the Blobstore interface
type InMemoryBlobstore struct {
	mutex    sync.Mutex
	blobs    map[string][]byte
	versions map[string]string
}

// NewInMemoryBlobstore creates an instance of an InMemoryBlobstore
func NewInMemoryBlobstore() *InMemoryBlobstore {
	return &InMemoryBlobstore{blobs: make(map[string][]byte), versions: make(map[string]string)}
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *InMemoryBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

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
func (bs *InMemoryBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	ver := uuid.New().String()
	data, err := ioutil.ReadAll(reader)

	if err != nil {
		return "", err
	}

	bs.blobs[key] = data
	bs.versions[key] = ver

	return ver, nil
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *InMemoryBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	ver, ok := bs.versions[key]
	check := !ok && expectedVersion == "" || ok && expectedVersion == ver

	if !check {
		return "", CheckAndPutError{key, expectedVersion, ver}
	}

	newVer := uuid.New().String()
	data, err := ioutil.ReadAll(reader)

	if err != nil {
		return "", err
	}

	bs.blobs[key] = data
	bs.versions[key] = newVer

	return newVer, nil
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// For InMemoryBlobstore instances error should never be returned (though other
// implementations of this interface can)
func (bs *InMemoryBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := bs.blobs[key]

	return ok, nil
}
