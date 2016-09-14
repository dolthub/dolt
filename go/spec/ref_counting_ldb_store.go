// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

type refCountingLdbStore struct {
	*chunks.LevelDBStore
	refCount int
	closeFn  func()
}

func newRefCountingLdbStore(path string, closeFn func()) *refCountingLdbStore {
	return &refCountingLdbStore{chunks.NewLevelDBStoreUseFlags(path, ""), 1, closeFn}
}

func (r *refCountingLdbStore) AddRef() {
	r.refCount++
}

func (r *refCountingLdbStore) Close() (err error) {
	d.PanicIfFalse(r.refCount > 0)
	r.refCount--
	if r.refCount == 0 {
		err = r.LevelDBStore.Close()
		r.closeFn()
	}
	return
}
