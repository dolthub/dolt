package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Package struct {
	types        []Type
	dependencies ref.RefSlice
	ref          *ref.Ref
}

func NewPackage(types []Type, deps ref.RefSlice) Package {
	// TODO: Check that |types| is sorted.
	d.Chk.True(sort.IsSorted(deps))
	return Package{types, deps, &ref.Ref{}}
}

func (p Package) Equals(other Value) bool {
	return other != nil && typeForPackage.Equals(other.Type()) && p.Ref() == other.Ref()
}

func (p Package) Ref() ref.Ref {
	return EnsureRef(p.ref, p)
}

func (p Package) Chunks() (chunks []ref.Ref) {
	for _, t := range p.types {
		chunks = append(chunks, t.Chunks()...)
	}
	for _, d := range p.dependencies {
		chunks = append(chunks, d)
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

func (p Package) Dependencies() ref.RefSlice {
	// TODO: Change API to prevent mutations.
	return p.dependencies
}

func (p Package) Types() []Type {
	// TODO: Change API to prevent mutations.
	return p.types
}
