package nbs

import (
	"bytes"

	"github.com/attic-labs/noms/go/blobstore"
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

func manifestVersionAndContents(bs blobstore.Blobstore) (string, manifestContents, error) {
	reader, ver, err := bs.Get(manifestFile, blobstore.AllRange)

	if err != nil {
		return "", manifestContents{}, err
	}

	defer reader.Close()
	return ver, parseManifest(reader), nil
}

// ParseIfExists looks for a manifest in the specified blobstore.  If one exists
// will return true and the contents, else false and nil
func (bsm blobstoreManifest) ParseIfExists(stats *Stats, readHook func()) (bool, manifestContents) {
	if readHook != nil {
		panic("Read hooks not supported")
	}

	_, contents, err := manifestVersionAndContents(bsm.bs)

	if err != nil {
		if blobstore.IsNotFoundError(err) {
			return false, contents
		}

		// io error.  Noms convention is to panic
		panic("Unable to read manifest due to error " + err.Error())
	}

	return true, contents
}

// Update updates the contents of the manifest in the blobstore
func (bsm blobstoreManifest) Update(lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents {
	if writeHook != nil {
		panic("Write hooks not supported")
	}

	ver, contents, err := manifestVersionAndContents(bsm.bs)

	if contents.lock == lastLock {
		buffer := bytes.NewBuffer(make([]byte, 64*1024)[:0])
		writeManifest(buffer, newContents)
		_, err = bsm.bs.CheckAndPut(ver, manifestFile, buffer)

		if err != nil {
			if !blobstore.IsCheckAndPutError(err) {
				// io error.  Noms convention is to panic
				panic("Unable to update manifest due to error " + err.Error())
			}
		} else {
			return newContents
		}
	}

	return contents
}
