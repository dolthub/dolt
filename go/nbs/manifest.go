// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"crypto/sha512"
	"strconv"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
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
	ParseIfExists(readHook func()) (
		exists bool,
		vers string,
		lock addr,
		root hash.Hash,
		tableSpecs []tableSpec,
	)

	// Update optimistically tries to write a new manifest containing
	// |newRoot| and the tables referenced by |specs|. If |lastLock| matches
	// the lock hash in the currently persisted manifest (logically, the lock
	// that would be returned by ParseIfExists), then Update succeeds and
	// subsequent calls to both Update and ParseIfExists will reflect a
	// manifest containing |newLock|, |newRoot| and |tables|. If not, Update
	// fails. Regardless, |lock|, |actual| and |tableSpecs| will reflect the
	// current state of the world upon return. Callers should check that
	// |actual| == |newRoot| and, if not, merge any desired new table
	// information with the contents of |tableSpecs| before trying again.
	// Concrete implementations are responsible for ensuring that concurrent
	// Update calls (and ParseIfExists calls) are correct.
	// If writeHook is non-nil, it will be invoked while the implementation is
	// guaranteeing exclusive access to the manifest. This allows for testing
	// of race conditions.
	Update(
		lastLock, newLock addr,
		specs []tableSpec,
		newRoot hash.Hash,
		writeHook func(),
	) (
		lock addr,
		actual hash.Hash,
		tableSpecs []tableSpec,
	)

	// Name returns a stable, unique identifier for the store this manifest describes.
	Name() string
}

type tableSpec struct {
	name       addr
	chunkCount uint32
}

func parseSpecs(tableInfo []string) []tableSpec {
	specs := make([]tableSpec, len(tableInfo)/2)
	for i := range specs {
		specs[i].name = ParseAddr([]byte(tableInfo[2*i]))
		c, err := strconv.ParseUint(tableInfo[2*i+1], 10, 32)
		d.PanicIfError(err)
		specs[i].chunkCount = uint32(c)
	}
	return specs
}

func formatSpecs(specs []tableSpec, tableInfo []string) {
	d.Chk.True(len(tableInfo) == 2*len(specs))
	for i, t := range specs {
		tableInfo[2*i] = t.name.String()
		tableInfo[2*i+1] = strconv.FormatUint(uint64(t.chunkCount), 10)
	}
}

// generateLockHash returns a hash of root and the names of all the tables in
// specs, which should be included in all persisted manifests. When a client
// attempts to update a manifest, it must check the lock hash in the currently
// persisted manifest against the lock hash it saw last time it loaded the
// contents of a manifest. If they do not match, the client must not update
// the persisted manifest.
func generateLockHash(root hash.Hash, specs []tableSpec) (lock addr) {
	blockHash := sha512.New()
	blockHash.Write(root[:])
	for _, spec := range specs {
		blockHash.Write(spec.name[:])
	}
	var h []byte
	h = blockHash.Sum(h) // Appends hash to h
	copy(lock[:], h)
	return
}
