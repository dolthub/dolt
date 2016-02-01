package datas

import "github.com/attic-labs/noms/chunks"

// Factory allows the creation of namespaced DataStore instances. The details of how namespaces are separated is left up to the particular implementation of Factory and DataStore.
type Factory interface {
	Create(string) (DataStore, bool)

	// Shutter shuts down the factory. Subsequent calls to Create() will fail.
	Shutter()
}

func (f Flags) CreateFactory() (Factory, bool) {
	var cf chunks.Factory
	if cf = f.ldb.CreateFactory(); cf != nil {
	} else if cf = f.dynamo.CreateFactory(); cf != nil {
	} else if cf = f.memory.CreateFactory(); cf != nil {
	}

	if cf != nil {
		return &localFactory{cf}, true
	}

	if cf = f.hflags.CreateFactory(); cf != nil {
		return &remoteFactory{cf}, true
	}
	return &localFactory{}, false
}

type localFactory struct {
	cf chunks.Factory
}

func (lf *localFactory) Create(ns string) (DataStore, bool) {
	if cs := lf.cf.CreateStore(ns); cs != nil {
		return newLocalDataStore(cs), true
	}
	return &LocalDataStore{}, false
}

func (lf *localFactory) Shutter() {
	lf.cf.Shutter()
}

type remoteFactory struct {
	cf chunks.Factory
}

func (rf *remoteFactory) Create(ns string) (DataStore, bool) {
	if cs := rf.cf.CreateStore(ns); cs != nil {
		return newRemoteDataStore(cs), true
	}
	return &LocalDataStore{}, false
}

func (rf *remoteFactory) Shutter() {
	rf.cf.Shutter()
}
