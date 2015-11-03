package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type structBuilderFunc func() chan Value

type toNomsValueFunc func(v Value) Value

var (
	packages         map[ref.Ref]*Package          = map[ref.Ref]*Package{}
	toNomsValueMap   map[ref.Ref]toNomsValueFunc   = map[ref.Ref]toNomsValueFunc{}
	structBuilderMap map[ref.Ref]structBuilderFunc = map[ref.Ref]structBuilderFunc{}
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

func readPackage(r ref.Ref, cs chunks.ChunkSource) *Package {
	p := ReadValue(r, cs).(Package)
	RegisterPackage(&p)
	return &p
}

func RegisterFromValFunction(t TypeRef, f toNomsValueFunc) {
	toNomsValueMap[t.Ref()] = f
}

func ToNomsValueFromTypeRef(t TypeRef, v Value) Value {
	if f, ok := toNomsValueMap[t.Ref()]; ok {
		return f(v)
	}
	return v
}

func RegisterStructBuilder(t TypeRef, f structBuilderFunc) {
	structBuilderMap[t.Ref()] = f
}

func structBuilderForTypeRef(typeRef, typeDef TypeRef) chan Value {
	if f, ok := structBuilderMap[typeRef.Ref()]; ok {
		return f()
	}
	return structBuilder(typeRef, typeDef)
}
