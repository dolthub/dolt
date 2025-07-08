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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/fslock"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const (
	manifestFileName = "manifest"
	lockFileName     = "LOCK"
	lockFileTimeout  = time.Millisecond * 100

	storageVersion4 = "4"

	prefixLen = 5
)

var ErrUnreadableManifest = errors.New("could not read file manifest")

type manifestChecker func(upstream, contents manifestContents) error

// ParseManifest parses a manifest file from the supplied reader
func ParseManifest(r io.Reader) (ManifestInfo, error) {
	return parseManifest(r)
}

func MaybeMigrateFileManifest(ctx context.Context, dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, manifestFileName))
	if os.IsNotExist(err) {
		// no manifest exists, no need to migrate
		return false, nil
	} else if err != nil {
		return false, err
	}

	_, contents, err := parseIfExists(ctx, dir, nil)
	if err != nil {
		return false, err
	}

	if contents.manifestVers == StorageVersion {
		// already on v5, no need to migrate
		return false, nil
	}

	check := func(_, contents manifestContents) error {
		if !contents.gcGen.IsEmpty() {
			return errors.New("migrating from v4 to v5 should result in a manifest with a 0 gcGen")
		}

		return nil
	}

	_, err = updateWithChecker(ctx, dir, syncFlush, check, contents.lock, contents, nil)

	if err != nil {
		return false, err
	}

	return true, err
}

// getFileManifest makes a new file manifest.
func getFileManifest(ctx context.Context, dir string, mode updateMode) (m manifest, err error) {
	lock := fslock.New(filepath.Join(dir, lockFileName))
	m = fileManifest{dir: dir, mode: mode, lock: lock}

	var f *os.File
	f, err = openIfExists(filepath.Join(dir, manifestFileName))
	if err != nil {
		return nil, err
	} else if f == nil {
		return m, nil
	}
	defer func() {
		// keep first error
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()

	var ok bool
	ok, _, err = m.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrUnreadableManifest
	}
	return
}

type updateMode byte

const (
	asyncFlush updateMode = 0
	syncFlush  updateMode = 1
)

type fileManifest struct {
	dir  string
	mode updateMode
	lock *fslock.Lock
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

func (fm fileManifest) Name() string {
	return fm.dir
}

// ParseIfExists looks for a LOCK and manifest file in fm.dir. If it finds
// them, it takes the lock, parses the manifest and returns its contents,
// setting |exists| to true. If not, it sets |exists| to false and returns. In
// that case, the other return values are undefined. If |readHook| is non-nil,
// it will be executed while ParseIfExists() holds the manifest file lock.
// This is to allow for race condition testing.
func (fm fileManifest) ParseIfExists(
	ctx context.Context,
	stats *Stats,
	readHook func() error,
) (exists bool, contents manifestContents, err error) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTimeSince(t1) }()

	// no file lock on the read path
	return parseIfExists(ctx, fm.dir, readHook)
}

func (fm fileManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	// hold the file lock while we update
	if err = tryFileLock(fm.lock); err != nil {
		return manifestContents{}, err
	}
	defer func() {
		if cerr := fm.lock.Unlock(); err == nil {
			err = cerr // keep first error
		}
	}()

	checker := func(upstream, contents manifestContents) error {
		if contents.gcGen != upstream.gcGen {
			return chunks.ErrGCGenerationExpired
		}
		return nil
	}

	return updateWithChecker(ctx, fm.dir, fm.mode, checker, lastLock, newContents, writeHook)
}

func (fm fileManifest) UpdateGCGen(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	// hold the file lock while we update
	if err = tryFileLock(fm.lock); err != nil {
		return manifestContents{}, err
	}
	defer func() {
		if cerr := fm.lock.Unlock(); err == nil {
			err = cerr // keep first error
		}
	}()

	return updateWithChecker(ctx, fm.dir, fm.mode, updateGCGenManifestCheck, lastLock, newContents, writeHook)
}

// parseV5Manifest parses the v5 manifest from the Reader given. Assumes the first field (the manifest version and
// following : character) have already been consumed by the reader.
//
// |-- String --|-- String --|-------- String --------|-------- String --------|-------- String -----------------|
// | nbs version:Noms version:Base32-encoded lock hash:Base32-encoded root hash:Base32-encoded GC generation hash
//
// |-- String --|- String --|...|-- String --|- String --|
// :table 1 hash:table 1 cnt:...:table N hash:table N cnt|
func parseV5Manifest(r io.Reader) (manifestContents, error) {
	manifest, err := io.ReadAll(r)

	if err != nil {
		return manifestContents{}, err
	}

	slices := strings.Split(string(manifest), ":")
	if len(slices) < prefixLen-1 || len(slices)%2 != 0 {
		return manifestContents{}, ErrCorruptManifest
	}

	specs, err := parseSpecs(slices[prefixLen-1:])
	if err != nil {
		return manifestContents{}, err
	}

	lock, ok := hash.MaybeParse(slices[1])
	if !ok {
		return manifestContents{}, fmt.Errorf("Could not parse lock hash: %s", slices[1])
	}

	gcGen, ok := hash.MaybeParse(slices[3])
	if !ok {
		return manifestContents{}, fmt.Errorf("Could not parse GC generation hash: %s", slices[3])
	}

	return manifestContents{
		manifestVers: StorageVersion,
		nbfVers:      slices[0],
		lock:         lock,
		root:         hash.Parse(slices[2]),
		gcGen:        gcGen,
		specs:        specs,
	}, nil
}

// parseManifest parses the manifest bytes in the reader given and returns the contents. Consumes the first few bytes
func parseManifest(r io.Reader) (manifestContents, error) {
	var version []byte
	buf := make([]byte, 1)

	// Parse the manifest up to the : character
	chars := 0
	for ; chars < 8; chars++ {
		_, err := r.Read(buf)
		if err != nil {
			return manifestContents{}, err
		}
		if buf[0] == ':' {
			break
		}
		version = append(version, buf[0])
	}
	if chars >= 8 {
		return manifestContents{}, ErrCorruptManifest
	}

	switch string(version) {
	case storageVersion4:
		return parseV4Manifest(r)
	case StorageVersion:
		return parseV5Manifest(r)
	default:
		return manifestContents{}, fmt.Errorf("Unknown manifest version: %s. You may need to update your client", string(version))
	}
}

func writeManifest(temp io.Writer, contents manifestContents) error {
	if len(contents.nbfVers) == 0 {
		return errors.New("runtime error: Noms format version cannot be empty")
	}
	if contents.lock.IsEmpty() {
		return errors.New("runtime error: Lock hash cannot be empty")
	}

	strs := make([]string, 2*len(contents.specs)+prefixLen)
	strs[0], strs[1], strs[2], strs[3], strs[4] = StorageVersion, contents.nbfVers, contents.lock.String(), contents.root.String(), contents.gcGen.String()
	tableInfo := strs[prefixLen:]
	formatSpecs(contents.specs, tableInfo)
	_, err := io.WriteString(temp, strings.Join(strs, ":"))

	return err
}

// parseV4Manifest parses the v4 manifest from the Reader given. Assumes the first field (the manifest version and
// following : character) have already been consumed by the reader.
//
// |-- String --|-- String --|-------- String --------|-------- String --------|-- String --|- String --|...|-- String --|- String --|
// | nbs version:Noms version:Base32-encoded lock hash:Base32-encoded root hash:table 1 hash:table 1 cnt:...:table N hash:table N cnt|
func parseV4Manifest(r io.Reader) (manifestContents, error) {
	manifest, err := io.ReadAll(r)

	if err != nil {
		return manifestContents{}, err
	}

	slices := strings.Split(string(manifest), ":")
	if len(slices) < 3 || len(slices)%2 == 0 {
		return manifestContents{}, ErrCorruptManifest
	}

	specs, err := parseSpecs(slices[3:])

	if err != nil {
		return manifestContents{}, err
	}

	ad, ok := hash.MaybeParse(slices[1])
	if !ok {
		return manifestContents{}, fmt.Errorf("Could not parse lock hash: %s", slices[1])
	}

	return manifestContents{
		manifestVers: storageVersion4,
		nbfVers:      slices[0],
		lock:         ad,
		root:         hash.Parse(slices[2]),
		specs:        specs,
	}, nil
}

// parseIfExists parses the manifest file if it exists, callers must hold the file lock.
func parseIfExists(_ context.Context, dir string, readHook func() error) (exists bool, contents manifestContents, err error) {
	if readHook != nil {
		if err = readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}

	var f *os.File
	if f, err = openIfExists(filepath.Join(dir, manifestFileName)); err != nil {
		return false, manifestContents{}, err
	} else if f == nil {
		return false, manifestContents{}, nil
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr // keep first error
		}
	}()

	contents, err = parseManifest(f)
	if err != nil {
		return false, contents, err
	}
	exists = true
	return
}

// updateWithChecker updates the manifest if |validate| is satisfied, callers must hold the file lock.
func updateWithChecker(_ context.Context, dir string, mode updateMode, validate manifestChecker, lastLock hash.Hash, newContents manifestContents, writeHook func() error) (mc manifestContents, err error) {
	var tempManifestPath string

	// Write a temporary manifest file, to be renamed over manifestFileName upon success.
	// The closure here ensures this file is closed before moving on.
	tempManifestPath, err = func() (name string, ferr error) {
		var temp *os.File
		temp, ferr = tempfiles.MovableTempFileProvider.NewFile(dir, "nbs_manifest_")
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

		if mode == syncFlush {
			if ferr = temp.Sync(); ferr != nil {
				return "", ferr
			}
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return manifestContents{}, err
	}

	defer file.Remove(tempManifestPath) // If we rename below, this will be a no-op

	// writeHook is for testing, allowing other code to slip in and try to do stuff while we hold the lock.
	if writeHook != nil {
		err = writeHook()

		if err != nil {
			return manifestContents{}, err
		}
	}

	var upstream manifestContents
	// Read current manifest (if it exists). The closure ensures that the file is closed before moving on, so we can rename over it later if need be.
	manifestPath := filepath.Join(dir, manifestFileName)
	upstream, err = func() (upstream manifestContents, ferr error) {
		if f, ferr := openIfExists(manifestPath); ferr == nil && f != nil {
			defer func() {
				closeErr := f.Close()

				if ferr == nil {
					ferr = closeErr
				}
			}()

			upstream, ferr = parseManifest(f)

			if ferr != nil {
				return manifestContents{}, ferr
			}

			if newContents.nbfVers != upstream.nbfVers {
				return manifestContents{}, errors.New("Update cannot change manifest version")
			}

			return upstream, nil
		} else if ferr != nil {
			return manifestContents{}, ferr
		}

		if !lastLock.IsEmpty() {
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

	// this is where we assert that gcGen is correct
	err = validate(upstream, newContents)

	if err != nil {
		return manifestContents{}, err
	}

	err = file.Rename(tempManifestPath, manifestPath)
	if err != nil {
		return manifestContents{}, err
	}

	if mode == syncFlush {
		if err = file.SyncDirectoryHandle(dir); err != nil {
			return manifestContents{}, err
		}
	}

	return newContents, nil
}

func tryFileLock(lock *fslock.Lock) (err error) {
	err = lock.LockWithTimeout(lockFileTimeout)
	if errors.Is(err, fslock.ErrTimeout) {
		err = errors.New("timed out reading database manifest")
	}
	return
}
