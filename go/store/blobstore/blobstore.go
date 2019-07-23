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
)

// Blobstore is an interface for storing and retrieving blobs of data by key
type Blobstore interface {
	Exists(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error)
	Put(ctx context.Context, key string, reader io.Reader) (string, error)
	CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error)
}

// GetBytes is a utility method calls bs.Get and handles reading the data from the returned
// io.ReadCloser and closing it.
func GetBytes(ctx context.Context, bs Blobstore, key string, br BlobRange) ([]byte, string, error) {
	rc, ver, err := bs.Get(ctx, key, br)

	if err != nil || rc == nil {
		return nil, ver, err
	}

	defer rc.Close()
	data, err := ioutil.ReadAll(rc)

	if err != nil {
		return nil, "", err
	}

	return data, ver, nil
}

// PutBytes is a utility method calls bs.Put by wrapping the supplied []byte in an io.Reader
func PutBytes(ctx context.Context, bs Blobstore, key string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	return bs.Put(ctx, key, reader)
}
