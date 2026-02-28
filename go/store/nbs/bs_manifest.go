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
	"fmt"
	"runtime"
	"time"

	"github.com/fatih/color"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func bsManifestCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "??:0"
	}
	// trim to last two path components for readability
	short := file
	for i := len(file) - 1; i >= 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			// find one more slash
			for j := i - 1; j >= 0; j-- {
				if file[j] == '/' {
					short = file[j+1:]
					break
				}
			}
			break
		}
	}
	return fmt.Sprintf("%s:%d", short, line)
}

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
	caller := bsManifestCaller(2)
	start := time.Now()
	defer func() {
		fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go] manifestVersionAndContents (caller=%s): elapsed: %s\n", caller, time.Since(start)))
	}()

	t := time.Now()
	reader, _, ver, err := bs.Get(ctx, manifestFile, blobstore.AllRange)
	fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go] manifestVersionAndContents.bs.Get(\"manifest\") (caller=%s): elapsed: %s\n", caller, time.Since(t)))

	if err != nil {
		return "", manifestContents{}, err
	}

	defer reader.Close()
	t = time.Now()
	contents, err := parseManifest(reader)
	fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go] manifestVersionAndContents.parseManifest (caller=%s): elapsed: %s\n", caller, time.Since(t)))

	if err != nil {
		return "", manifestContents{}, err
	}

	return ver, contents, nil
}

// ParseIfExists looks for a manifest in the specified blobstore.  If one exists
// will return true and the contents, else false and nil
func (bsm blobstoreManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	caller := bsManifestCaller(2)
	start := time.Now()
	defer func() {
		fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go:ParseIfExists] (caller=%s): elapsed: %s\n", caller, time.Since(start)))
	}()

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
func (bsm blobstoreManifest) Update(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	checker := func(upstream, contents manifestContents) error {
		if contents.gcGen != upstream.gcGen {
			return chunks.ErrGCGenerationExpired
		}
		return nil
	}

	return updateBSWithChecker(ctx, behavior, bsm.bs, checker, lastLock, newContents, writeHook)
}

func (bsm blobstoreManifest) UpdateGCGen(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	return updateBSWithChecker(ctx, behavior, bsm.bs, updateGCGenManifestCheck, lastLock, newContents, writeHook)
}

func updateBSWithChecker(ctx context.Context, behavior dherrors.FatalBehavior, bs blobstore.Blobstore, validate manifestChecker, lastLock hash.Hash, newContents manifestContents, writeHook func() error) (mc manifestContents, err error) {
	caller := bsManifestCaller(2)
	start := time.Now()
	defer func() {
		fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go:updateBSWithChecker] (caller=%s): elapsed: %s\n", caller, time.Since(start)))
	}()

	if writeHook != nil {
		panic("Write hooks not supported")
	}

	t := time.Now()
	ver, contents, err := manifestVersionAndContents(ctx, bs)
	fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go:updateBSWithChecker] manifestVersionAndContents (read current) (caller=%s): elapsed: %s\n", caller, time.Since(t)))

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

		t = time.Now()
		_, err = bs.CheckAndPut(ctx, ver, manifestFile, int64(buffer.Len()), buffer)
		fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go:updateBSWithChecker] bs.CheckAndPut(\"manifest\") (caller=%s): elapsed: %s\n", caller, time.Since(t)))

		if err != nil {
			if !blobstore.IsCheckAndPutError(err) {
				return manifestContents{}, err
			}
			// CheckAndPut failed due to concurrent modification. Re-read
			// the manifest so we return current contents rather than the
			// stale (possibly empty) contents from before the race. Without
			// this, an empty manifestContents can be cached and poison
			// subsequent Fetch() calls (nbfVers="").
			t = time.Now()
			_, contents, err = manifestVersionAndContents(ctx, bs)
			fmt.Fprint(color.Output, fmt.Sprintf("[bs_manifest.go:updateBSWithChecker] manifestVersionAndContents (re-read after CAS fail) (caller=%s): elapsed: %s\n", caller, time.Since(t)))
			if err != nil && !blobstore.IsNotFoundError(err) {
				return manifestContents{}, err
			}
		} else {
			return newContents, nil
		}
	}

	return contents, nil
}
