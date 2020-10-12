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

package nbs

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/store/blobstore"
)

const (
	manifestFile = "manifest"
)

type blobstoreManifest struct {
	name string
	bs   blobstore.Blobstore
}

func (bsm blobstoreManifest) Name() string {
	return bsm.name
}

func manifestVersionAndContents(ctx context.Context, bs blobstore.Blobstore) (string, manifestContents, error) {
	reader, ver, err := bs.Get(ctx, manifestFile, blobstore.AllRange)

	if err != nil {
		return "", manifestContents{}, err
	}

	defer reader.Close()
	contents, err := fileManifestV5{}.parseManifest(reader)

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
func (bsm blobstoreManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if writeHook != nil {
		panic("Write hooks not supported")
	}

	ver, contents, err := manifestVersionAndContents(ctx, bsm.bs)

	if err != nil {
		return manifestContents{}, err
	}

	if contents.lock == lastLock {
		buffer := bytes.NewBuffer(make([]byte, 64*1024)[:0])
		err := fileManifestV5{}.writeManifest(buffer, newContents)

		if err != nil {
			return manifestContents{}, err
		}

		_, err = bsm.bs.CheckAndPut(ctx, ver, manifestFile, buffer)

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

func writeVer4Manifest(temp io.Writer, contents manifestContents) error {
	strs := make([]string, 2*len(contents.specs)+4)
	strs[0], strs[1], strs[2], strs[3] = storageVersion4, contents.vers, contents.lock.String(), contents.root.String()
	tableInfo := strs[4:]
	formatSpecs(contents.specs, tableInfo)
	_, err := io.WriteString(temp, strings.Join(strs, ":"))

	return err
}
