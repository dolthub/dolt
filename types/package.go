package types

import "github.com/attic-labs/noms/ref"

type Package struct {
	types        []TypeRef
	dependencies []ref.Ref
	ref          *ref.Ref
}

func NewPackage(types []TypeRef, deps []ref.Ref) Package {
	return Package{types, deps, &ref.Ref{}}
}

func (p Package) Equals(other Value) bool {
	if other, ok := other.(Package); ok {
		return p.Ref() == other.Ref()
	}
	return false
}

func (p Package) Ref() ref.Ref {
	return ensureRef(p.ref, p)
}

func (p Package) Chunks() (futures []Future) {
	for _, t := range p.types {
		futures = append(futures, t.Chunks()...)
	}
	for _, d := range p.dependencies {
		futures = append(futures, futureFromRef(d))
	}
	return
}

var typeRefForPackage = MakePrimitiveTypeRef(PackageKind)

func (p Package) TypeRef() TypeRef {
	return typeRefForPackage
}

func (p Package) GetOrdinal(n string) (ordinal int64) {
	for i, t := range p.types {
		if t.Name() == n && t.Namespace() == "" {
			return int64(i)
		}
	}
	return -1
}

func (p Package) Dependencies() []ref.Ref {
	// TODO: Change API to prevent mutations.
	return p.dependencies
}

func (p Package) Types() []TypeRef {
	// TODO: Change API to prevent mutations.
	return p.types
}
