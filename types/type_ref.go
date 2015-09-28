package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// TypeRef structs define and describe Noms types, both custom and built-in.
// pkgRef is optional. If set, then pkgRef + name address a type defined in another package.
// name is required for StructKind and EnumKind types, and may be allowed for others if we do type aliases. Named types are 'exported' in that they can be addressed from other type packages.
// Desc describes the referenced type. It may contain only a types.NomsKind, in the case of primitives, or it may contain additional information -- e.g. element TypeRefs for compound type specializations, field descriptions for structs, etc. Either way, checking Desc.Kind() allows code to understand how to interpret the rest of the data.
// If Kind() refers to a primitive, then Desc is empty.
// If Kind() refers to List, Set or Ref, then Desc is a TypeRef describing the element type.
// If Kind() refers to Map, then Desc is a 2-element List(TypeRef), describing the key and value types respectively.
// If Kind() refers to Struct, then Desc is {"fields": [name, type, ...], "choices": [name, type, ...]}.
// If Kind() refers to Enum, then Desc is a List(String) describing the enumerated values.
type TypeRef struct {
	pkgRef *Ref
	name   name
	Desc   TypeDesc

	ref *ref.Ref
}

type name struct {
	namespace, name string
}

func (n name) compose() (out string) {
	d.Chk.True(n.namespace == "" || (n.namespace != "" && n.name != ""), "If a TypeRef's namespace is set, so must name be.")
	if n.namespace != "" {
		out = n.namespace + "."
	}
	if n.name != "" {
		out += n.name
	}
	return
}

// IsUnresolved returns true if t doesn't contain description information. The caller should look the type up by Name in the NamedTypes of the appropriate Package.
func (t TypeRef) IsUnresolved() bool {
	_, ok := t.Desc.(UnresolvedDesc)
	return ok
}

// Describe() methods generate text that should parse into the struct being described.
// TODO: Figure out a way that they can exist only in the test file.
func (t TypeRef) Describe() (out string) {
	if t.name != (name{}) {
		out += t.name.compose() + "\n"
	}
	if !t.IsUnresolved() {
		out += t.Desc.Describe() + "\n"
	}
	return
}

func (t TypeRef) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t TypeRef) PackageRef() Ref {
	if t.pkgRef == nil {
		return Ref{}
	}
	return *t.pkgRef
}

func (t TypeRef) Name() string {
	return t.name.name
}

func (t TypeRef) NamespacedName() string {
	return t.name.compose()
}

func (t TypeRef) Namespace() string {
	return t.name.namespace
}

func (t TypeRef) Ref() ref.Ref {
	return ensureRef(t.ref, t)
}

func (t TypeRef) Equals(other Value) (res bool) {
	if other, ok := other.(TypeRef); ok {
		return t.Ref() == other.Ref()
	}
	return false
}

func (t TypeRef) Chunks() (out []Future) {
	if t.pkgRef != nil {
		out = append(out, futureFromRef(t.pkgRef.Ref()))
	}
	v := t.Desc.ToValue()
	if v != nil {
		out = append(out, v.Chunks()...)
	}
	return
}

func MakePrimitiveTypeRef(k NomsKind) TypeRef {
	return buildType("", PrimitiveDesc(k))
}

func MakePrimitiveTypeRefByString(p string) TypeRef {
	return buildType("", primitiveToDesc(p))
}

func MakeCompoundTypeRef(name string, kind NomsKind, elemTypes ...TypeRef) TypeRef {
	if len(elemTypes) == 1 {
		d.Chk.NotEqual(MapKind, kind, "MapKind requires 2 element types.")
	} else {
		d.Chk.Equal(MapKind, kind)
		d.Chk.Len(elemTypes, 2, "MapKind requires 2 element types.")
	}
	return buildType(name, CompoundDesc{kind, elemTypes})
}

func MakeEnumTypeRef(name string, ids ...string) TypeRef {
	return buildType(name, EnumDesc{ids})
}

func MakeStructTypeRef(name string, fields []Field, choices Choices) TypeRef {
	return buildType(name, StructDesc{fields, choices})
}

func MakeTypeRef(n string, pkg Ref) TypeRef {
	return TypeRef{name: name{name: n}, pkgRef: &pkg, Desc: UnresolvedDesc{}, ref: &ref.Ref{}}
}

func MakeExternalTypeRef(namespace, n string) TypeRef {
	return TypeRef{name: name{namespace, n}, Desc: UnresolvedDesc{}, ref: &ref.Ref{}}
}

func buildType(n string, desc TypeDesc) TypeRef {
	if IsPrimitiveKind(desc.Kind()) {
		return TypeRef{name: name{name: n}, Desc: desc, ref: &ref.Ref{}}
	}
	switch desc.Kind() {
	case ListKind, RefKind, SetKind, MapKind, EnumKind, StructKind:
		return TypeRef{name: name{name: n}, Desc: desc, ref: &ref.Ref{}}
	default:
		d.Exp.Fail("Unrecognized Kind:", "%v", desc.Kind())
		panic("unreachable")
	}
}
