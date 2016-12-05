// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
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

type manifest interface {
	// ParseIfExists extracts and returns values from a NomsBlockStore
	// manifest, if one exists. Concrete implementations are responsible for
	// defining how to find and parse the desired manifest, e.g. a
	// particularly-named file in a given directory. Implementations are also
	// responsible for managing whatever concurrency guarantees they require
	// for correctness.
	// If the manifest exists, |exists| is set to true and manifest data is
	// returned, including the version of the Noms data in the store, the root
	// hash.Hash of the store, and a tableSpec describing every table that
	// comprises the store.
	// If the manifest doesn't exist, |exists| is set to false and the other
	// return values are undefined. The |readHook| parameter allows race
	// condition testing. If it is non-nil, it will be invoked while the
	// implementation is guaranteeing exclusive access to the manifest.
	ParseIfExists(readHook func()) (exists bool, vers string, root hash.Hash, tableSpecs []tableSpec)

	// Update optimistically tries to write a new manifest containing
	// |newRoot| and the tables referenced by |specs|. If |root| matches the root
	// hash in the currently persisted manifest (logically, the root that
	// would be returned by ParseIfExists), then Update succeeds and
	// subsequent calls to both Update and ParseIfExists will reflect a
	// manifest containing |newRoot| and |tables|. If not, Update fails.
	// Regardless, |actual| and |tableSpecs| will reflect the current state of
	// the world upon return. Callers should check that |actual| == |newRoot|
	// and, if not, merge any desired new table information with the contents
	// of |tableSpecs| before trying again.
	// Concrete implementations are responsible for ensuring that concurrent
	// Update calls (and ParseIfExists calls) are correct.
	// If writeHook is non-nil, it will be invoked while the implementation is
	// guaranteeing exclusive access to the manifest. This allows for testing
	// of race conditions.
	Update(specs []tableSpec, root, newRoot hash.Hash, writeHook func()) (actual hash.Hash, tableSpecs []tableSpec)
}

// fileManifest provides access to a NomsBlockStore manifest stored on disk in |dir|. The format
// is currently human readable:
//
// |-- String --|-- String --|-------- String --------|-- String --|- String --|...|-- String --|- String --|
// | nbs version:Noms version:Base32-encoded root hash:table 1 hash:table 1 cnt:...:table N hash:table N cnt|
type fileManifest struct {
	dir string
}

type tableSpec struct {
	name       addr
	chunkCount uint32
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

	slices := bytes.Split(manifest, []byte(":"))
	if len(slices) < 3 || len(slices)%2 == 0 {
		d.Chk.Fail("Malformed manifest: " + string(manifest))
	}
	d.PanicIfFalse(StorageVersion == string(slices[0]))

	tableInfo := slices[3:]
	specs := make([]tableSpec, len(tableInfo)/2)
	for i := range specs {
		specs[i].name = ParseAddr(tableInfo[2*i])
		c, err := strconv.ParseUint(string(tableInfo[2*i+1]), 10, 32)
		d.PanicIfError(err)
		specs[i].chunkCount = uint32(c)
	}

	return string(slices[1]), hash.Parse(string(slices[2])), specs
}

func writeManifest(temp io.Writer, root hash.Hash, specs []tableSpec) {
	strs := make([]string, 2*len(specs)+3)
	strs[0], strs[1], strs[2] = StorageVersion, constants.NomsVersion, root.String()
	tableInfo := strs[3:]
	for i, t := range specs {
		tableInfo[2*i] = t.name.String()
		tableInfo[2*i+1] = strconv.FormatUint(uint64(t.chunkCount), 10)
	}
	_, err := io.WriteString(temp, strings.Join(strs, ":"))
	d.PanicIfError(err)
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

func checkClose(c io.Closer) {
	d.PanicIfError(c.Close())
}

func flock(lockFilePath string) io.Closer {
	l, err := os.Create(lockFilePath)
	d.PanicIfError(err)
	d.PanicIfError(unix.Flock(int(l.Fd()), unix.LOCK_EX))
	return l
}
