package datas

import "github.com/attic-labs/noms/chunks"

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
