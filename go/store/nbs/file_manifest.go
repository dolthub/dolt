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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/juju/fslock"

	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/util/tempfiles"
)

const (
	manifestFileName = "manifest"
	lockFileName     = "LOCK"
)

// fileManifest provides access to a NomsBlockStore manifest stored on disk in |dir|. The format
// is currently human readable:
//
// |-- String --|-- String --|-------- String --------|-------- String --------|-- String --|- String --|...|-- String --|- String --|
// | nbs version:Noms version:Base32-encoded lock hash:Base32-encoded root hash:table 1 hash:table 1 cnt:...:table N hash:table N cnt|
type fileManifest struct {
	dir string
}

func newLock(dir string) *fslock.Lock {
	lockPath := filepath.Join(dir, lockFileName)
	return fslock.New(lockPath)
}

func lockFileExists(dir string) (bool, error) {
	lockPath := filepath.Join(dir, lockFileName)
	info, err := os.Stat(lockPath)

	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, errors.New("failed to determine if lock file exists")
	} else if info.IsDir() {
		return false, errors.New("lock file is a directory")
	}

	return true, nil
}

func (fm fileManifest) Name() string {
	return fm.dir
}

// ParseIfExists looks for a LOCK and manifest file in fm.dir. If it finds
// them, it takes the lock, parses the manifest and returns its contents,
// setting |exists| to true. If not, it sets |exists| to false and returns. In
// that case, the other return values are undefined. If |readHook| is non-nil,
// it will be executed while ParseIfExists() holds the manifest file lock.
// This is to allow for race condition testing.
func (fm fileManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (exists bool, contents manifestContents, err error) {
	t1 := time.Now()
	defer func() {
		stats.ReadManifestLatency.SampleTimeSince(t1)
	}()

	var locked bool
	locked, err = lockFileExists(fm.dir)

	if err != nil {
		return false, manifestContents{}, err
	}

	// !exists(lockFileName) => unitialized store
	if locked {
		var f io.ReadCloser
		err = func() (ferr error) {
			lck := newLock(fm.dir)
			ferr = lck.Lock()

			if ferr != nil {
				return ferr
			}

			defer func() {
				unlockErr := lck.Unlock()

				if ferr == nil {
					ferr = unlockErr
				}
			}()

			if readHook != nil {
				ferr = readHook()

				if ferr != nil {
					return ferr
				}
			}

			f, ferr = openIfExists(filepath.Join(fm.dir, manifestFileName))

			if ferr != nil {
				return ferr
			}

			return nil
		}()

		if err != nil {
			return exists, contents, err
		}

		if f != nil {
			defer func() {
				closeErr := f.Close()

				if err == nil {
					err = closeErr
				}
			}()

			exists = true

			contents, err = parseManifest(f)

			if err != nil {
				return false, contents, err
			}
		}
	}

	return exists, contents, nil
}

// Returns nil if path does not exist
func openIfExists(path string) (*os.File, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return f, err
}

// ParseManifest parses s a manifest file from the supplied reader
func ParseManifest(r io.Reader) (ManifestInfo, error) {
	return parseManifest(r)
}

func parseManifest(r io.Reader) (manifestContents, error) {
	manifest, err := ioutil.ReadAll(r)

	if err != nil {
		return manifestContents{}, err
	}

	slices := strings.Split(string(manifest), ":")
	if len(slices) < 4 || len(slices)%2 == 1 {
		return manifestContents{}, ErrCorruptManifest
	}

	if StorageVersion != string(slices[0]) {
		return manifestContents{}, errors.New("invalid storage version")
	}

	specs, err := parseSpecs(slices[4:])

	if err != nil {
		return manifestContents{}, err
	}

	ad, err := parseAddr([]byte(slices[2]))

	if err != nil {
		return manifestContents{}, err
	}

	return manifestContents{
		vers:  slices[1],
		lock:  ad,
		root:  hash.Parse(slices[3]),
		specs: specs,
	}, nil
}

func (fm fileManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	var tempManifestPath string

	// Write a temporary manifest file, to be renamed over manifestFileName upon success.
	// The closure here ensures this file is closed before moving on.
	tempManifestPath, err = func() (name string, ferr error) {
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFile.NewFile(fm.dir, "nbs_manifest_")

		if ferr != nil {
			return "", ferr
		}

		defer func() {
			closeErr := temp.Close()

			if ferr == nil {
				ferr = closeErr
			}
		}()

		ferr = writeManifest(temp, newContents)

		if ferr != nil {
			return "", ferr
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return manifestContents{}, err
	}

	defer os.Remove(tempManifestPath) // If we rename below, this will be a no-op

	// Take manifest file lock
	lck := newLock(fm.dir)
	err = lck.Lock()

	if err != nil {
		return manifestContents{}, err
	}

	defer func() {
		unlockErr := lck.Unlock()

		if err == nil {
			err = unlockErr
		}
	}()

	// writeHook is for testing, allowing other code to slip in and try to do stuff while we hold the lock.
	if writeHook != nil {
		err = writeHook()

		if err != nil {
			return manifestContents{}, err
		}
	}

	var upstream manifestContents
	// Read current manifest (if it exists). The closure ensures that the file is closed before moving on, so we can rename over it later if need be.
	manifestPath := filepath.Join(fm.dir, manifestFileName)
	upstream, err = func() (upstream manifestContents, ferr error) {
		if f, ferr := openIfExists(manifestPath); ferr == nil && f != nil {
			defer func() {
				closeErr := f.Close()

				if ferr != nil {
					ferr = closeErr
				}
			}()

			upstream, ferr = parseManifest(f)

			if ferr != nil {
				return manifestContents{}, ferr
			}

			if newContents.vers != upstream.vers {
				return manifestContents{}, errors.New("Update cannot change manifest version")
			}

			return upstream, nil
		} else if ferr != nil {
			return manifestContents{}, ferr
		}

		if lastLock != (addr{}) {
			return manifestContents{}, errors.New("new manifest created with non 0 lock")
		}

		return manifestContents{}, nil
	}()

	if err != nil {
		return manifestContents{}, err
	}

	if lastLock != upstream.lock {
		return upstream, nil
	}

	err = os.Rename(tempManifestPath, manifestPath)

	if err != nil {
		return manifestContents{}, err
	}

	return newContents, nil
}

func writeManifest(temp io.Writer, contents manifestContents) error {
	strs := make([]string, 2*len(contents.specs)+4)
	strs[0], strs[1], strs[2], strs[3] = StorageVersion, contents.vers, contents.lock.String(), contents.root.String()
	tableInfo := strs[4:]
	formatSpecs(contents.specs, tableInfo)
	_, err := io.WriteString(temp, strings.Join(strs, ":"))

	return err
}
