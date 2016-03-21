package types

import (
	"sort"

	"github.com/attic-labs/noms/ref"
)

type Package struct {
	types        []Type
	dependencies ref.RefSlice
	ref          *ref.Ref
}

func NewPackage(types []Type, dependencies ref.RefSlice) Package {
	p := Package{types: types, ref: &ref.Ref{}}
	// The order |Package.dependencies| must be stable for the Package to have a stable ref.
	// See https://github.com/attic-labs/noms/issues/814 for stable ordering of |Package.types|.
	p.dependencies = append(p.dependencies, dependencies...)
	sort.Sort(p.dependencies)
	return p
}

func (p Package) Equals(other Value) bool {
	return other != nil && typeForPackage.Equals(other.Type()) && p.Ref() == other.Ref()
}

func (p Package) Ref() ref.Ref {
	return EnsureRef(p.ref, p)
}

func (p Package) Chunks() (chunks []RefBase) {
	for _, t := range p.types {
		chunks = append(chunks, t.Chunks()...)
	}
	for _, d := range p.dependencies {
		chunks = append(chunks, refFromType(d, MakeRefType(typeForPackage)))
	}
	return
}

func (p Package) ChildValues() (res []Value) {
	for _, t := range p.types {
		res = append(res, t)
	}
	for _, d := range p.dependencies {
		res = append(res, NewRefOfPackage(d))
	}
	return
}

var typeForPackage = MakePrimitiveType(PackageKind)

func (p Package) Type() Type {
	return typeForPackage
}

func (p Package) GetOrdinal(n string) (ordinal int64) {
	for i, t := range p.types {
		if t.Name() == n && t.Namespace() == "" {
			return int64(i)
		}
	}
	return -1
}

func (p Package) Dependencies() (dependencies []ref.Ref) {
	dependencies = append(dependencies, p.dependencies...)
	return
}

func (p Package) Types() (types []Type) {
	types = append(types, p.types...)
	return
}
