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

package nbs

import (
	"bytes"
	"context"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	manifestFile = "manifest"
)

type blobstoreManifest struct {
	bs blobstore.Blobstore
}

func (bsm blobstoreManifest) Name() string {
	return bsm.bs.Path()
}

func manifestVersionAndContents(ctx context.Context, bs blobstore.Blobstore) (string, manifestContents, error) {
	reader, ver, err := bs.Get(ctx, manifestFile, blobstore.AllRange)

	if err != nil {
		return "", manifestContents{}, err
	}

	defer reader.Close()
	contents, err := parseManifest(reader)

	if err != nil {
		return "", manifestContents{}, err
	}

	return ver, contents, nil
}

// ParseIfExists looks for a manifest in the specified blobstore.  If one exists
// will return true and the contents, else false and nil
func (bsm blobstoreManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	if readHook != nil {
		panic("Read hooks not supported")
	}

	_, contents, err := manifestVersionAndContents(ctx, bsm.bs)

	if err != nil {
		if blobstore.IsNotFoundError(err) {
			return false, contents, nil
		}

		// io error
		return true, contents, err
	}

	return true, contents, nil
}

// Update updates the contents of the manifest in the blobstore
func (bsm blobstoreManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	checker := func(upstream, contents manifestContents) error {
		if contents.gcGen != upstream.gcGen {
			return chunks.ErrGCGenerationExpired
		}
		return nil
	}

	return updateBSWithChecker(ctx, bsm.bs, checker, lastLock, newContents, writeHook)
}

func (bsm blobstoreManifest) UpdateGCGen(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	return updateBSWithChecker(ctx, bsm.bs, updateGCGenManifestCheck, lastLock, newContents, writeHook)
}

func updateBSWithChecker(ctx context.Context, bs blobstore.Blobstore, validate manifestChecker, lastLock hash.Hash, newContents manifestContents, writeHook func() error) (mc manifestContents, err error) {
	if writeHook != nil {
		panic("Write hooks not supported")
	}

	ver, contents, err := manifestVersionAndContents(ctx, bs)

	if err != nil && !blobstore.IsNotFoundError(err) {
		return manifestContents{}, err
	}

	// this is where we assert that gcGen is correct
	err = validate(contents, newContents)
	if err != nil {
		return manifestContents{}, err
	}

	if contents.lock == lastLock {
		buffer := bytes.NewBuffer(make([]byte, 64*1024)[:0])
		err := writeManifest(buffer, newContents)

		if err != nil {
			return manifestContents{}, err
		}

		_, err = bs.CheckAndPut(ctx, ver, manifestFile, int64(buffer.Len()), buffer)

		if err != nil {
			if !blobstore.IsCheckAndPutError(err) {
				return manifestContents{}, err
			}
		} else {
			return newContents, nil
		}
	}

	return contents, nil
}
