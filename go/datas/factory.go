// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import "github.com/attic-labs/noms/go/chunks"

// Factory allows the creation of namespaced Database instances. The details of how namespaces are separated is left up to the particular implementation of Factory and Database.
type Factory interface {
	Create(string) (Database, bool)

	// Shutter shuts down the factory. Subsequent calls to Create() will fail.
	Shutter()
}

type localFactory struct {
	cf chunks.Factory
}

func (lf *localFactory) Create(ns string) (Database, bool) {
	if cs := lf.cf.CreateStore(ns); cs != nil {
		return newLocalDatabase(cs), true
	}
	return &LocalDatabase{}, false
}

func (lf *localFactory) Shutter() {
	lf.cf.Shutter()
}
