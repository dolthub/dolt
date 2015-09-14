package parse

import (
	"fmt"
	"io"
	"strings"

	"github.com/attic-labs/noms/d"
)

// ParsePackage reads a Noms package specification from r and returns a Package. Errors will be annotated with logname and thrown.
func ParsePackage(logname string, r io.Reader) Package {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(Package)
}

// A Package has a map of import Aliases that are not yet really used.
// UsingDeclarations is kind of a hack to indicate specializations of Noms containers that need to be generated. These should all be one of ListKind, SetKind, MapKind or RefKind, and Desc should be a CompoundDesc instance.
// NamedTypes is a lookup table for types defined in this package. These should all be EnumKind or StructKind. When traversing the definition of a given type, you may run into a TypeRef that IsUnresolved(). In that case, look it up by name in the NamedTypes of the appropriate package.
type Package struct {
	Aliases           map[string]string
	UsingDeclarations []TypeRef
	NamedTypes        map[string]TypeRef
}

// TypeRef represents a possibly-symbolic reference to a type.
// PkgRef will be some kind of reference to another Noms type package, almost certainly a ref.Ref.
// Name is required for StructKind and EnumKind types, and may be allowed for others if we do type aliases. Named types are 'exported' in that they can be addressed from other type packages.
// Desc describes the referenced type. It may contain only a NomsKind, in the case of primitives, or it may contain additional information -- e.g.element TypeRefs for compound type specializations, field descriptions for structs, etc. Either way, checking Desc.Kind() allows code to understand how to interpret the rest of the data.
// NB: If we weren't looking towards Nomifying these datastructures, we'd just type-switch on Desc instead of using Kind().
type TypeRef struct {
	PkgRef string // Not yet used.
	Name   string
	Desc   TypeDesc // When Noms-ified, the TypeRef handling code will use Kind information to know how to deserialize Desc.
}

// IsUnresolved returns true if t doesn't contain description information. The caller should look the type up by Name in the NamedTypes of the appropriate Package.
func (t TypeRef) IsUnresolved() bool {
	return t.Desc == nil
}

func (t TypeRef) Equals(other TypeRef) bool {
	if t.IsUnresolved() {
		return t.PkgRef == other.PkgRef && t.Name == other.Name
	}
	return t.PkgRef == other.PkgRef && t.Name == other.Name && t.Desc.Equals(other.Desc)
}

// The describe() methods generate text that should parse into the struct being described.
// TODO: Figure out a way that they can exist only in the test file.
func (t TypeRef) describe() string {
	seg := []string{}
	if t.Name != "" {
		seg = append(seg, t.Name)
	}
	if t.Desc != nil {
		seg = append(seg, t.Desc.describe())
	}
	return strings.Join(seg, "\n")
}

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Int32, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	Equals(other TypeDesc) bool
	describe() string // For use in tests.
}

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind int

// All supported kinds of Noms types are enumerated here.
const (
	BoolKind NomsKind = iota
	UInt8Kind
	UInt16Kind
	UInt32Kind
	UInt64Kind
	Int8Kind
	Int16Kind
	Int32Kind
	Int64Kind
	Float32Kind
	Float64Kind
	StringKind
	BlobKind
	ValueKind
	ListKind
	MapKind
	RefKind
	SetKind
	UnionKind
	EnumKind
	StructKind
)

func makePrimitiveTypeRef(p string) TypeRef {
	return TypeRef{Desc: primitiveToDesc[p]}
}

// PrimitiveDesc implements TypeDesc for all primitive Noms types:
// Bool
// UInt8
// UInt16
// UInt32
// UInt64
// Int8
// Int16
// Int32
// Int64
// Float32
// Float64
// String
// Blob
// Value
type PrimitiveDesc NomsKind

func (p PrimitiveDesc) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveDesc) Equals(other TypeDesc) bool {
	return p.Kind() == other.Kind()
}

func (p PrimitiveDesc) describe() string {
	for k, v := range primitiveToDesc {
		if p == v {
			return k
		}
	}
	panic("Not reachable.")
}

var primitiveToDesc = map[string]PrimitiveDesc{
	"Bool":    PrimitiveDesc(BoolKind),
	"UInt64":  PrimitiveDesc(UInt64Kind),
	"UInt32":  PrimitiveDesc(UInt32Kind),
	"UInt16":  PrimitiveDesc(UInt16Kind),
	"UInt8":   PrimitiveDesc(UInt8Kind),
	"Int64":   PrimitiveDesc(Int64Kind),
	"Int32":   PrimitiveDesc(Int32Kind),
	"Int16":   PrimitiveDesc(Int16Kind),
	"Int8":    PrimitiveDesc(Int8Kind),
	"Float64": PrimitiveDesc(Float64Kind),
	"Float32": PrimitiveDesc(Float32Kind),
	"String":  PrimitiveDesc(StringKind),
	"Blob":    PrimitiveDesc(BlobKind),
	"Value":   PrimitiveDesc(ValueKind),
}

func makeCompoundTypeRef(k NomsKind, e []TypeRef) TypeRef {
	return TypeRef{Desc: CompoundDesc{k, e}}
}

// CompoundDesc describes a List, Map, Set or Ref type.
// ElemTypes indicates what type or types are in the container indicated by kind, e.g. Map key and value or Set element.
type CompoundDesc struct {
	kind      NomsKind
	ElemTypes []TypeRef
}

func (c CompoundDesc) Kind() NomsKind {
	return c.kind
}

func (c CompoundDesc) Equals(other TypeDesc) bool {
	if c.Kind() != other.Kind() {
		return false
	}
	out := true
	for i, e := range other.(CompoundDesc).ElemTypes {
		out = out && e.Equals(c.ElemTypes[i])
	}
	return out
}

func (c CompoundDesc) describe() string {
	descElems := func() string {
		out := make([]string, len(c.ElemTypes))
		for i, e := range c.ElemTypes {
			if e.Name != "" {
				out[i] = e.Name
			} else {
				out[i] = e.Desc.describe()
			}
		}
		return strings.Join(out, ", ")
	}
	switch c.kind {
	case ListKind:
		return "List(" + descElems() + ")"
	case MapKind:
		return "Map(" + descElems() + ")"
	case RefKind:
		return "Ref(" + descElems() + ")"
	case SetKind:
		return "Set(" + descElems() + ")"
	default:
		panic(fmt.Errorf("Kind is not compound: %v", c.kind))
	}
}

func makeEnumTypeRef(n string, ids []string) TypeRef {
	return TypeRef{Name: n, Desc: EnumDesc{ids}}
}

// EnumDesc simply lists the identifiers used in this enum.
type EnumDesc struct {
	IDs []string
}

func (e EnumDesc) Kind() NomsKind {
	return EnumKind
}

func (e EnumDesc) Equals(other TypeDesc) bool {
	if e.Kind() != other.Kind() {
		return false
	}
	out := true
	for i, id := range other.(EnumDesc).IDs {
		out = out && id == e.IDs[i]
	}
	return out
}

func (e EnumDesc) describe() string {
	return "enum: { " + strings.Join(e.IDs, "\n") + "}\n"
}

func makeUnionTypeRef(c []Field) TypeRef {
	return TypeRef{Desc: UnionDesc{c}}
}

// UnionDesc represents each choice as a Field, akin to a StructDesc.
type UnionDesc struct {
	Choices []Field
}

func (u UnionDesc) Kind() NomsKind {
	return UnionKind
}

func (u UnionDesc) Equals(other TypeDesc) bool {
	if u.Kind() != other.Kind() || len(u.Choices) != len(other.(UnionDesc).Choices) {
		return false
	}
	for i, c := range other.(UnionDesc).Choices {
		if !u.Choices[i].Equals(c) {
			return false
		}
	}
	return true
}

func (u UnionDesc) describe() (out string) {
	out = "union {\n"
	for _, c := range u.Choices {
		out += fmt.Sprintf("  %s :%s\n", c.Name, c.T.describe())
	}
	return out + "  }"
}

// Field represents a Struct field or a Union choice.
// Neither Name nor T is allowed to be a zero-value, though T may be an unresolved TypeRef.
type Field struct {
	Name string
	T    TypeRef
}

func (f Field) Equals(other Field) bool {
	return f.Name == other.Name && f.T.Equals(other.T)
}

func makeStructTypeRef(n string, f []Field, u *UnionDesc) TypeRef {
	return TypeRef{Name: n, Desc: StructDesc{f, u}}
}

// StructDesc describes a custom Noms Struct.
// Structs can contain at most one anonymous union, so Union may be nil.
type StructDesc struct {
	Fields []Field
	Union  *UnionDesc
}

func (s StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) Equals(other TypeDesc) bool {
	if s.Kind() != other.Kind() || len(s.Fields) != len(other.(StructDesc).Fields) {
		return false
	}
	for i, f := range other.(StructDesc).Fields {
		if !s.Fields[i].Equals(f) {
			return false
		}
	}
	return true
}

func (s StructDesc) describe() (out string) {
	out = ""
	if len(s.Union.Choices) != 0 {
		out += fmt.Sprintf("  anon %s\n", s.Union.describe())
	}
	for _, f := range s.Fields {
		out += fmt.Sprintf("  %s: %s\n", f.Name, f.T.describe())
	}
	return
}

func makeTypeRef(pkgRef, n string) TypeRef {
	return TypeRef{PkgRef: pkgRef, Name: n}
}
