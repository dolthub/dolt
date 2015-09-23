package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var packages map[ref.Ref]*Package = map[ref.Ref]*Package{}

// LookupPackage looks for a Package by ref.Ref in the global cache of Noms type packages.
func LookupPackage(r ref.Ref) *Package {
	return packages[r]
}

// RegisterPackage puts p into the global cache of Noms type packages.
func RegisterPackage(p *Package) (r ref.Ref) {
	d.Chk.NotNil(p)
	r = p.Ref()
	packages[r] = p
	return
}
