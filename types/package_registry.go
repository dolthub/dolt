package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type toNomsValueFunc func(v Value) NomsValue

var (
	packages       map[ref.Ref]*Package        = map[ref.Ref]*Package{}
	toNomsValueMap map[ref.Ref]toNomsValueFunc = map[ref.Ref]toNomsValueFunc{}
)

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

func RegisterFromValFunction(t TypeRef, f toNomsValueFunc) {
	toNomsValueMap[t.Ref()] = f
}

func ToNomsValueFromTypeRef(t TypeRef, v Value) NomsValue {
	f, ok := toNomsValueMap[t.Ref()]
	d.Chk.True(ok)
	return f(v)
}
