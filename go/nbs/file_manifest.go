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
// |-- String --|-- String --|-------- String --------|-- String --|- String --|...|-- String --|- String --|
// | nbs version:Noms version:Base32-encoded root hash:table 1 hash:table 1 cnt:...:table N hash:table N cnt|
type fileManifest struct {
	dir string
}

// ParseIfExists looks for a LOCK and manifest file in fm.dir. If it finds
// them, it takes the lock, parses the manifest and returns its contents,
// setting |exists| to true. If not, it sets |exists| to false and returns. In
// that case, the other return values are undefined. If |readHook| is non-nil,
// it will be executed while ParseIfExists() holds the manfiest file lock.
// This is to allow for race condition testing.
func (fm fileManifest) ParseIfExists(readHook func()) (exists bool, vers string, root hash.Hash, tableSpecs []tableSpec) {
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
			vers, root, tableSpecs = parseManifest(f)
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

func parseManifest(r io.Reader) (string, hash.Hash, []tableSpec) {
	manifest, err := ioutil.ReadAll(r)
	d.PanicIfError(err)

	slices := strings.Split(string(manifest), ":")
	if len(slices) < 3 || len(slices)%2 == 0 {
		d.Chk.Fail("Malformed manifest: " + string(manifest))
	}
	d.PanicIfFalse(StorageVersion == string(slices[0]))

	return slices[1], hash.Parse(slices[2]), parseSpecs(slices[3:])
}

// Update optimistically tries to write a new manifest, containing |newRoot|
// and the elements of |tables|. If the existing manifest on disk doesn't
// contain |root|, Update fails and returns the parsed contents of the
// manifest on disk. Callers should check that |actual| == |newRoot| upon
// return and, if not, merge any desired new table information with the
// contents of |tableSpecs| before trying again.
// If writeHook is non-nil, it will be invoked wile the manifest file lock is
// held. This is to allow for testing of race conditions.
func (fm fileManifest) Update(specs []tableSpec, root, newRoot hash.Hash, writeHook func()) (actual hash.Hash, tableSpecs []tableSpec) {
	tableSpecs = specs

	// Write a temporary manifest file, to be renamed over manifestFileName upon success.
	// The closure here ensures this file is closed before moving on.
	tempManifestPath := func() string {
		temp, err := ioutil.TempFile(fm.dir, "nbs_manifest_")
		d.PanicIfError(err)
		defer checkClose(temp)
		writeManifest(temp, newRoot, tableSpecs)
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
			mVers, actual, tableSpecs = parseManifest(f)
			d.PanicIfFalse(constants.NomsVersion == mVers)
		} else {
			d.Chk.True(root == hash.Hash{})
		}
	}()

	if root != actual {
		return actual, tableSpecs
	}
	err := os.Rename(tempManifestPath, manifestPath)
	d.PanicIfError(err)
	return newRoot, tableSpecs
}

func writeManifest(temp io.Writer, root hash.Hash, specs []tableSpec) {
	strs := make([]string, 2*len(specs)+3)
	strs[0], strs[1], strs[2] = StorageVersion, constants.NomsVersion, root.String()
	tableInfo := strs[3:]
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
