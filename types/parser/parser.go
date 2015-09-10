package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/attic-labs/noms/d"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: grammar 'EXPR'")
	}

	err := d.Try(func() {
		file, err := os.Open(os.Args[1])
		d.Exp.NoError(err)
		defer file.Close()
		pkg := ParsePackage(os.Args[1], file)
		// lookup := buildLookupTable(pkg)
		//resolvePackage(pkg, lookup)

		for _, decl := range pkg.UsingDeclarations {
			d.Exp.IsType(CompoundDesc{}, decl.Desc)
			fmt.Println(decl.Explain())
		}
		for _, named := range pkg.NamedTypes {
			fmt.Println(named.Explain())
		}
	})

	if err != nil {
		log.Fatal(err)
	}
}

func ParsePackage(logname string, r io.Reader) Package {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(Package)
}

// A Package has a Name, and a map of import Aliases that are not yet really used.
// UsingDeclarations is kind of a hack to indicate specializations of Noms containers that need to be generated. These should all be one of ListKind, SetKind, MapKind or RefKind, and Desc should be a CompoundDesc instance.
// NamedTypes is a lookup table for types defined in this package. These should all be EnumKind or StructKind. When traversing the definition of a given type, you may run into a TypeRef that IsUnresolved(). In that case, look it up by name in the NamedTypes of the appropriate package.
type Package struct {
	Name              string
	Aliases           map[string]string
	UsingDeclarations []TypeRef
	NamedTypes        map[string]TypeRef
}

type TypeRef struct {
	PkgRef string // Not yet used.
	Name   string
	Desc   TypeDesc // When Noms-ified, the TypeRef handling code will use Kind information to know how to deserialize Desc.
}

func (t TypeRef) IsUnresolved() bool {
	return t.Desc == nil
}

func (t TypeRef) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t TypeRef) Explain() string {
	seg := []string{}
	if t.Name != "" {
		seg = append(seg, fmt.Sprintf("NamedType %s: ", t.Name))
	}
	if t.Desc != nil {
		seg = append(seg, t.Desc.Describe())
	}
	return strings.Join(seg, "\n")
}

type TypeDesc interface {
	Kind() NomsKind
	Describe() string
}

type NomsKind int

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

func MakePrimitiveTypeRef(p string) TypeRef {
	return TypeRef{Desc: PrimitiveToDesc[p]}
}

type PrimitiveDesc NomsKind

func (p PrimitiveDesc) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveDesc) Describe() string {
	for k, v := range PrimitiveToDesc {
		if p == v {
			return k
		}
	}
	panic("wha")
}

var PrimitiveToDesc = map[string]PrimitiveDesc{
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

func MakeCompoundTypeRef(k NomsKind, e []TypeRef) TypeRef {
	return TypeRef{Desc: CompoundDesc{k, e}}
}

type CompoundDesc struct {
	kind      NomsKind
	elemTypes []TypeRef
}

func (c CompoundDesc) Kind() NomsKind {
	return c.kind
}

func (c CompoundDesc) Describe() string {
	descElems := func() (out string) {
		for _, e := range c.elemTypes {
			out += fmt.Sprintf("%v (%T); ", e, e.Desc)
		}
		return out
	}
	switch c.kind {
	case ListKind:
		return "List of " + descElems()
	case MapKind:
		return "Map of " + descElems()
	case RefKind:
		return "Ref of " + descElems()
	case SetKind:
		return "Set of " + descElems()
	default:
		panic("ffuu")
	}
}

func MakeEnumTypeRef(n string, ids []string) TypeRef {
	return TypeRef{Name: n, Desc: EnumDesc{ids}}
}

type EnumDesc struct {
	ids []string
}

func (e EnumDesc) Kind() NomsKind {
	return EnumKind
}

func (e EnumDesc) Describe() string {
	return "Enum: " + strings.Join(e.ids, ", ")
}

func MakeUnionTypeRef(c []Field) TypeRef {
	return TypeRef{Desc: UnionDesc{c}}
}

type UnionDesc struct {
	choices []Field
}

func (n UnionDesc) Kind() NomsKind {
	return UnionKind
}

func (u UnionDesc) Describe() (out string) {
	out = "Union of {\n"
	for _, c := range u.choices {
		out += fmt.Sprintf("  %s, %s\n", c.Name, c.t.Explain())
	}
	return out + "  }"
}

type Field struct {
	Name string
	t    TypeRef
}

type StructDesc struct {
	fields []Field
	union  UnionDesc
}

func MakeStructTypeRef(n string, f []Field, u UnionDesc) TypeRef {
	return TypeRef{Name: n, Desc: StructDesc{f, u}}
}

func (e StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) Describe() (out string) {
	out = ""
	if len(s.union.choices) != 0 {
		out += fmt.Sprintf("  anon %s\n", s.union.Describe())
	}
	for _, f := range s.fields {
		out += fmt.Sprintf("  %s: %s\n", f.Name, f.t.Explain())
	}
	return
}

type alias struct {
	Name   string
	Target string
}

// I think these aren't the right appproach
/*func resolvePackage(pkg *Package, lookup map[string]*TypeRef) {
	d.Chk.NotNil(pkg)
	for _, decl := range pkg.UsingDeclarations {
		d.Exp.IsType(CompoundDesc{}, decl.Desc)
		for i, e := range decl.Desc.(CompoundDesc).elemTypes {
			if e.IsUnresolved() {
				if tr, ok := lookup[e.Name]; ok {
					decl.Desc.(CompoundDesc).elemTypes[i] = *tr
				}
			}
		}
	}
	for _, named := range pkg.NamedTypes {
		if named.Kind() == StructKind {
			d.Exp.IsType(StructDesc{}, named.Desc)
			resolveFields(named.Desc.(StructDesc).fields, lookup)
			resolveFields(named.Desc.(StructDesc).union.choices, lookup)
		}
	}
}

func buildLookupTable(pkgs ...Package) map[string]*TypeRef {
	out := map[string]*TypeRef{}
	for _, pkg := range pkgs {
		for i, n := range pkg.NamedTypes {
			out[n.Name] = &pkg.NamedTypes[i]
		}
	}
	return out
}

func resolveFields(fields []Field, lookup map[string]*TypeRef) {
	for i, f := range fields {
		if f.t.IsUnresolved() {
			if tr, ok := lookup[f.t.Name]; ok {
				fields[i] = Field{f.Name, *tr}
			}
		} else if f.t.Kind() == UnionKind {
			d.Exp.IsType(UnionDesc{}, f.t.Desc)
			resolveFields(f.t.Desc.(UnionDesc).choices, lookup)
		}
	}
}*/
