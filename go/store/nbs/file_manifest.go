// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"github.com/juju/fslock"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
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

func lockFileExists(dir string) bool {
	lockPath := filepath.Join(dir, lockFileName)
	info, err := os.Stat(lockPath)

	if err != nil {
		if os.IsNotExist(err) {
			return false
		}

		// When in rome
		d.Panic("Failed to determine if lock file exists")
	} else if info.IsDir() {
		d.Panic("Lock file is a directory")
	}

	return true
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
func (fm fileManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func()) (exists bool, contents manifestContents) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTimeSince(t1) }()

	// !exists(lockFileName) => unitialized store
	if lockFileExists(fm.dir) {
		var f io.ReadCloser
		func() {
			lck := newLock(fm.dir)
			d.PanicIfError(lck.Lock())
			defer lck.Unlock()

			if readHook != nil {
				readHook()
			}
			f = openIfExists(filepath.Join(fm.dir, manifestFileName))
		}()

		if f != nil {
			defer checkClose(f)
			exists = true
			contents = parseManifest(f)
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

// ParseManifest parses s a manifest file from the supplied reader
func ParseManifest(r io.Reader) ManifestInfo {
	return parseManifest(r)
}

func parseManifest(r io.Reader) manifestContents {
	manifest, err := ioutil.ReadAll(r)
	d.PanicIfError(err)

	slices := strings.Split(string(manifest), ":")
	if len(slices) < 4 || len(slices)%2 == 1 {
		d.Chk.Fail("Malformed manifest: " + string(manifest))
	}
	d.PanicIfFalse(StorageVersion == string(slices[0]))

	return manifestContents{
		vers:  slices[1],
		lock:  ParseAddr([]byte(slices[2])),
		root:  hash.Parse(slices[3]),
		specs: parseSpecs(slices[4:]),
	}
}

func (fm fileManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	// Write a temporary manifest file, to be renamed over manifestFileName upon success.
	// The closure here ensures this file is closed before moving on.
	tempManifestPath := func() string {
		temp, err := ioutil.TempFile(fm.dir, "nbs_manifest_")
		d.PanicIfError(err)
		defer checkClose(temp)
		writeManifest(temp, newContents)
		return temp.Name()
	}()
	defer os.Remove(tempManifestPath) // If we rename below, this will be a no-op

	// Take manifest file lock
	lck := newLock(fm.dir)
	d.PanicIfError(lck.Lock())
	defer lck.Unlock()

	// writeHook is for testing, allowing other code to slip in and try to do stuff while we hold the lock.
	if writeHook != nil {
		writeHook()
	}

	// Read current manifest (if it exists). The closure ensures that the file is closed before moving on, so we can rename over it later if need be.
	manifestPath := filepath.Join(fm.dir, manifestFileName)
	upstream := func() manifestContents {
		if f := openIfExists(manifestPath); f != nil {
			defer checkClose(f)

			upstream := parseManifest(f)
			d.PanicIfFalse(constants.NomsVersion == upstream.vers)
			return upstream
		}
		d.Chk.True(lastLock == addr{})
		return manifestContents{}
	}()

	if lastLock != upstream.lock {
		return upstream
	}
	rerr := os.Rename(tempManifestPath, manifestPath)
	d.PanicIfError(rerr)
	return newContents
}

func writeManifest(temp io.Writer, contents manifestContents) {
	strs := make([]string, 2*len(contents.specs)+4)
	strs[0], strs[1], strs[2], strs[3] = StorageVersion, contents.vers, contents.lock.String(), contents.root.String()
	tableInfo := strs[4:]
	formatSpecs(contents.specs, tableInfo)
	_, err := io.WriteString(temp, strings.Join(strs, ":"))
	d.PanicIfError(err)
}

func checkClose(c io.Closer) {
	d.PanicIfError(c.Close())
}
