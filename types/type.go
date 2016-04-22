package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// Type defines and describes Noms types, both custom and built-in.
// StructKind types, and possibly others if we do type aliases, will have a Name(). Named types are 'exported' in that they can be addressed from other type packages.
// Desc provides more details of the type. It may contain only a types.NomsKind, in the case of primitives, or it may contain additional information -- e.g. element Types for compound type specializations, field descriptions for structs, etc. Either way, checking Kind() allows code to understand how to interpret the rest of the data.
// If Kind() refers to a primitive, then Desc has no more info.
// If Kind() refers to List, Map, Set or Ref, then Desc is a list of Types describing the element type(s).
// If Kind() refers to Struct, then Desc contains a []Field and Choices.
// If Kind() refers to an UnresolvedKind, then Desc contains a PackageRef, which is the Ref of the package where the type definition is defined. The ordinal, if not -1, is the index into the Types list of the package. If the Name is set then the ordinal needs to be found.

type Type struct {
	name name
	Desc TypeDesc

	ref *ref.Ref
}

type name struct {
	namespace, name string
}

func (n name) compose() (out string) {
	d.Chk.True(n.namespace == "" || (n.namespace != "" && n.name != ""), "If a Type's namespace is set, so must name be.")
	if n.namespace != "" {
		out = n.namespace + "."
	}
	if n.name != "" {
		out += n.name
	}
	return
}

// IsUnresolved returns true if t doesn't contain description information. The caller should look the type up by Ordinal in the Types of the appropriate Package.
func (t *Type) IsUnresolved() bool {
	_, ok := t.Desc.(UnresolvedDesc)
	return ok
}

func (t *Type) HasPackageRef() bool {
	return t.IsUnresolved() && !t.PackageRef().IsEmpty()
}

// Describe generate text that should parse into the struct being described.
func (t *Type) Describe() (out string) {
	return WriteHRS(t)
}

func (t *Type) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t *Type) IsOrdered() bool {
	switch t.Desc.Kind() {
	case Float32Kind, Float64Kind, Int8Kind, Int16Kind, Int32Kind, Int64Kind, Uint8Kind, Uint16Kind, Uint32Kind, Uint64Kind, StringKind, RefKind:
		return true
	default:
		return false
	}
}

func (t *Type) PackageRef() ref.Ref {
	desc, ok := t.Desc.(UnresolvedDesc)
	d.Chk.True(ok, "PackageRef only works on unresolved types")
	return desc.pkgRef
}

func (t *Type) Ordinal() int16 {
	d.Chk.True(t.HasOrdinal(), "Ordinal has not been set")
	return t.Desc.(UnresolvedDesc).ordinal
}

func (t *Type) HasOrdinal() bool {
	return t.IsUnresolved() && t.Desc.(UnresolvedDesc).ordinal >= 0
}

func (t *Type) Name() string {
	return t.name.name
}

func (t *Type) Namespace() string {
	return t.name.namespace
}

func (t *Type) Ref() ref.Ref {
	return EnsureRef(t.ref, t)
}

func (t *Type) Equals(other Value) (res bool) {
	return other != nil && t.Ref() == other.Ref()
}

func (t *Type) Chunks() (chunks []RefBase) {
	if t.IsUnresolved() {
		if t.HasPackageRef() {
			chunks = append(chunks, refFromType(t.PackageRef(), MakeRefType(typeForPackage)))
		}
		return
	}
	if desc, ok := t.Desc.(CompoundDesc); ok {
		for _, t := range desc.ElemTypes {
			chunks = append(chunks, t.Chunks()...)
		}
	}
	return
}

func (t *Type) ChildValues() (res []Value) {
	if t.HasPackageRef() {
		res = append(res, NewTypedRef(MakeRefType(PackageType), t.PackageRef()))
	}
	if !t.IsUnresolved() {
		switch desc := t.Desc.(type) {
		case CompoundDesc:
			for _, t := range desc.ElemTypes {
				res = append(res, t)
			}
		case StructDesc:
			for _, t := range desc.Fields {
				res = append(res, t.T)
			}
			for _, t := range desc.Union {
				res = append(res, t.T)
			}
		case UnresolvedDesc:
			// Nothing, this is handled by the HasPackageRef() check above
		case PrimitiveDesc:
			// Nothing, these have no child values
		default:
			d.Chk.Fail("Unexpected type desc implementation: %#v", t)
		}
	}
	return
}

var typeForType = MakePrimitiveType(TypeKind)

func (t *Type) Type() *Type {
	return typeForType
}

func MakePrimitiveType(k NomsKind) *Type {
	return buildType("", PrimitiveDesc(k))
}

func MakePrimitiveTypeByString(p string) *Type {
	return buildType("", primitiveToDesc(p))
}

func MakeCompoundType(kind NomsKind, elemTypes ...*Type) *Type {
	if len(elemTypes) == 1 {
		d.Chk.NotEqual(MapKind, kind, "MapKind requires 2 element types.")
		d.Chk.True(kind == RefKind || kind == ListKind || kind == SetKind)
	} else {
		d.Chk.Equal(MapKind, kind)
		d.Chk.Len(elemTypes, 2, "MapKind requires 2 element types.")
	}
	return buildType("", CompoundDesc{kind, elemTypes})
}

func MakeStructType(name string, fields []Field, choices []Field) *Type {
	return buildType(name, StructDesc{fields, choices})
}

func MakeType(pkgRef ref.Ref, ordinal int16) *Type {
	d.Chk.True(ordinal >= 0)
	return &Type{Desc: UnresolvedDesc{pkgRef, ordinal}, ref: &ref.Ref{}}
}

func MakeUnresolvedType(namespace, n string) *Type {
	return &Type{name: name{namespace, n}, Desc: UnresolvedDesc{ordinal: -1}, ref: &ref.Ref{}}
}

func MakeListType(elemType *Type) *Type {
	return buildType("", CompoundDesc{ListKind, []*Type{elemType}})
}

func MakeSetType(elemType *Type) *Type {
	return buildType("", CompoundDesc{SetKind, []*Type{elemType}})
}

func MakeMapType(keyType, valType *Type) *Type {
	return buildType("", CompoundDesc{MapKind, []*Type{keyType, valType}})
}

func MakeRefType(elemType *Type) *Type {
	return buildType("", CompoundDesc{RefKind, []*Type{elemType}})
}

func buildType(n string, desc TypeDesc) *Type {
	if IsPrimitiveKind(desc.Kind()) {
		return &Type{name: name{name: n}, Desc: desc, ref: &ref.Ref{}}
	}
	switch desc.Kind() {
	case ListKind, RefKind, SetKind, MapKind, StructKind, UnresolvedKind:
		return &Type{name: name{name: n}, Desc: desc, ref: &ref.Ref{}}
	default:
		d.Exp.Fail("Unrecognized Kind:", "%v", desc.Kind())
		panic("unreachable")
	}
}

var Uint8Type = MakePrimitiveType(Uint8Kind)
var Uint16Type = MakePrimitiveType(Uint16Kind)
var Uint32Type = MakePrimitiveType(Uint32Kind)
var Uint64Type = MakePrimitiveType(Uint64Kind)
var Int8Type = MakePrimitiveType(Int8Kind)
var Int16Type = MakePrimitiveType(Int16Kind)
var Int32Type = MakePrimitiveType(Int32Kind)
var Int64Type = MakePrimitiveType(Int64Kind)
var Float32Type = MakePrimitiveType(Float32Kind)
var Float64Type = MakePrimitiveType(Float64Kind)
var BoolType = MakePrimitiveType(BoolKind)
var StringType = MakePrimitiveType(StringKind)
var BlobType = MakePrimitiveType(BlobKind)
var PackageType = MakePrimitiveType(PackageKind)
