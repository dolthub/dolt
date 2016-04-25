package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var (
	packages map[ref.Ref]*Package = map[ref.Ref]*Package{}
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

func ReadPackage(r ref.Ref, vr ValueReader) *Package {
	p := vr.ReadValue(r).(Package)
	RegisterPackage(&p)
	return &p
}

type Package struct {
	types        []*Type
	dependencies ref.RefSlice
	ref          *ref.Ref
}

func NewPackage(types []*Type, dependencies ref.RefSlice) Package {
	p := Package{types: types}
	// The order |Package.dependencies| must be stable for the Package to have a stable ref.
	// See https://github.com/attic-labs/noms/issues/814 for stable ordering of |Package.types|.
	p.dependencies = append(p.dependencies, dependencies...)
	sort.Sort(p.dependencies)
	r := getRef(p)
	p.ref = &r

	newTypes := make([]*Type, len(types))
	for i, t := range types {
		newTypes[i] = FixupType(t, &p)
	}
	p.types = newTypes

	return p
}

func (p Package) Equals(other Value) bool {
	return other != nil && typeForPackage.Equals(other.Type()) && p.Ref() == other.Ref()
}

func (p Package) Ref() ref.Ref {
	return EnsureRef(p.ref, p)
}

func (p Package) Chunks() (chunks []Ref) {
	for _, t := range p.types {
		chunks = append(chunks, t.Chunks()...)
	}
	for _, d := range p.dependencies {
		chunks = append(chunks, NewTypedRef(MakeRefType(typeForPackage), d))
	}
	return
}

func (p Package) ChildValues() (res []Value) {
	for _, t := range p.types {
		res = append(res, t)
	}
	for _, d := range p.dependencies {
		res = append(res, NewTypedRef(p.Type(), d))
	}
	return
}

var typeForPackage = PackageType
var typeForRefOfPackage = MakeRefType(PackageType)

func (p Package) Type() *Type {
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

func (p Package) Types() (types []*Type) {
	types = append(types, p.types...)
	return
}

func NewSetOfRefOfPackage() Set {
	return NewTypedSet(MakeSetType(typeForRefOfPackage))
}
