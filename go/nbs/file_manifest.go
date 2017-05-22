// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
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

func (fm fileManifest) Name() string {
	return fm.dir
}

// ParseIfExists looks for a LOCK and manifest file in fm.dir. If it finds
// them, it takes the lock, parses the manifest and returns its contents,
// setting |exists| to true. If not, it sets |exists| to false and returns. In
// that case, the other return values are undefined. If |readHook| is non-nil,
// it will be executed while ParseIfExists() holds the manfiest file lock.
// This is to allow for race condition testing.
func (fm fileManifest) ParseIfExists(stats *Stats, readHook func()) (exists bool, vers string, lock addr, root hash.Hash, tableSpecs []tableSpec) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTime(time.Since(t1)) }()

	// !exists(lockFileName) => unitialized store
	if l := openIfExists(filepath.Join(fm.dir, lockFileName)); l != nil {
		var f io.ReadCloser
		func() {
			d.PanicIfError(unix.Flock(int(l.Fd()), unix.LOCK_EX))
			defer checkClose(l) // releases the flock()

			if readHook != nil {
				readHook()
			}
			f = openIfExists(filepath.Join(fm.dir, manifestFileName))
		}()

		if f != nil {
			defer checkClose(f)
			exists = true
			vers, lock, root, tableSpecs = parseManifest(f)
		}
	}
	return
}

// Returns nil if path does not exist
func openIfExists(path string) *os.File {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	d.PanicIfError(err)
	return f
}

func parseManifest(r io.Reader) (nomsVersion string, lock addr, root hash.Hash, specs []tableSpec) {
	manifest, err := ioutil.ReadAll(r)
	d.PanicIfError(err)

	slices := strings.Split(string(manifest), ":")
	if len(slices) < 4 || len(slices)%2 == 1 {
		d.Chk.Fail("Malformed manifest: " + string(manifest))
	}
	d.PanicIfFalse(StorageVersion == string(slices[0]))

	return slices[1], ParseAddr([]byte(slices[2])), hash.Parse(slices[3]), parseSpecs(slices[4:])
}

func (fm fileManifest) Update(lastLock, newLock addr, specs []tableSpec, newRoot hash.Hash, stats *Stats, writeHook func()) (lock addr, actual hash.Hash, tableSpecs []tableSpec) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTime(time.Since(t1)) }()

	// Write a temporary manifest file, to be renamed over manifestFileName upon success.
	// The closure here ensures this file is closed before moving on.
	tempManifestPath := func() string {
		temp, err := ioutil.TempFile(fm.dir, "nbs_manifest_")
		d.PanicIfError(err)
		defer checkClose(temp)
		writeManifest(temp, newLock, newRoot, specs)
		return temp.Name()
	}()
	defer os.Remove(tempManifestPath) // If we rename below, this will be a no-op

	// Take manifest file lock
	defer checkClose(flock(filepath.Join(fm.dir, lockFileName))) // closing releases the lock

	// writeHook is for testing, allowing other code to slip in and try to do stuff while we hold the lock.
	if writeHook != nil {
		writeHook()
	}

	// Read current manifest (if it exists). The closure ensures that the file is closed before moving on, so we can rename over it later if need be.
	manifestPath := filepath.Join(fm.dir, manifestFileName)
	func() {
		if f := openIfExists(manifestPath); f != nil {
			defer checkClose(f)

			var mVers string
			mVers, lock, actual, tableSpecs = parseManifest(f)
			d.PanicIfFalse(constants.NomsVersion == mVers)
		} else {
			d.Chk.True(lastLock == addr{})
		}
	}()

	if lastLock != lock {
		return lock, actual, tableSpecs
	}
	rerr := os.Rename(tempManifestPath, manifestPath)
	d.PanicIfError(rerr)
	return newLock, newRoot, specs
}

func writeManifest(temp io.Writer, lock addr, root hash.Hash, specs []tableSpec) {
	strs := make([]string, 2*len(specs)+4)
	strs[0], strs[1], strs[2], strs[3] = StorageVersion, constants.NomsVersion, lock.String(), root.String()
	tableInfo := strs[4:]
	formatSpecs(specs, tableInfo)
	_, err := io.WriteString(temp, strings.Join(strs, ":"))
	d.PanicIfError(err)
}

func checkClose(c io.Closer) {
	d.PanicIfError(c.Close())
}

func flock(lockFilePath string) io.Closer {
	l, err := os.Create(lockFilePath)
	d.PanicIfError(err)
	d.PanicIfError(unix.Flock(int(l.Fd()), unix.LOCK_EX))
	return l
}
