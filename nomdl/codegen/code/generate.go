// Package code provides Generator, which has methods for generating code snippets from a types.Type.
// Conceptually there are few type spaces here:
//
// - Def - MyStructDef, ListOfBoolDef; convenient Go types for working with data from a given Noms Value.
// - Native - such as string, uint32
// - Value - the generic types.Value
// - Nom - types.String, types.Uint32, MyStruct, ListOfBool
// - User - User defined structs, enums etc as well as native primitves. This uses Native when possible or Nom if not. These are to be used in APIs for generated types -- Getters and setters for maps and structs, etc.
package code

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Resolver provides a single method for resolving an unresolved types.Type.
type Resolver interface {
	Resolve(t types.Type, pkg *types.Package) types.Type
}

// Generator provides methods for generating code snippets from both resolved and unresolved types.Types. In the latter case, it uses R to resolve the types.Type before generating code.
type Generator struct {
	R               Resolver
	TypesPackage    string
	ImportedJS      map[string]bool
	ImportedJSTypes map[string]bool
	AliasNames      map[ref.Ref]string
	Package         *types.Package
}

// DefType returns a string containing the Go type that should be used as the 'Def' for the Noms type described by t.
func (gen *Generator) DefType(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%sBlob", gen.TypesPackage)
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return strings.ToLower(kindToString(k))
	case types.EnumKind:
		return gen.UserName(t)
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return gen.UserName(t) + "Def"
	case types.PackageKind:
		return fmt.Sprintf("%sPackage", gen.TypesPackage)
	case types.RefKind:
		return "ref.Ref"
	case types.ValueKind:
		return fmt.Sprintf("%sValue", gen.TypesPackage)
	case types.TypeKind:
		return fmt.Sprintf("%sType", gen.TypesPackage)
	}
	panic("unreachable")
}

// UserType returns a string containing the Go type that should be used when the Noms type described by t needs to be returned by a generated getter or taken as a parameter to a generated setter.
func (gen *Generator) UserType(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%sBlob", gen.TypesPackage)
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return strings.ToLower(kindToString(k))
	case types.EnumKind, types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return gen.UserName(t)
	case types.PackageKind:
		return fmt.Sprintf("%sPackage", gen.TypesPackage)
	case types.ValueKind:
		return fmt.Sprintf("%sValue", gen.TypesPackage)
	case types.TypeKind:
		return fmt.Sprintf("%sType", gen.TypesPackage)
	}
	panic("unreachable")
}

// UserTypeJS returns a string containing the JS type that should be used when the Noms type described by t needs to be returned by a generated getter or taken as a parameter to a generated setter.
func (gen *Generator) UserTypeJS(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return gen.ImportJSType("Blob")
	case types.BoolKind:
		return "boolean"
	case types.StringKind:
		return "string"
	case types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return gen.ImportJSType(strings.ToLower(kindToString(k)))
	case types.EnumKind, types.StructKind:
		if t.HasPackageRef() && gen.Package.Ref() != t.PackageRef() {
			return gen.importedUserNameJS(t)
		}
		return gen.UserName(t)
	case types.ListKind:
		return fmt.Sprintf("%s<%s>", gen.ImportJSType("NomsList"), gen.UserTypeJS(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.SetKind:
		return fmt.Sprintf("%s<%s>", gen.ImportJSType("NomsSet"), gen.UserTypeJS(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.RefKind:
		return fmt.Sprintf("%s<%s>", gen.ImportJSType("RefValue"), gen.UserTypeJS(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.MapKind:
		elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
		return fmt.Sprintf("%s<%s, %s>", gen.ImportJSType("NomsMap"), gen.UserTypeJS(elemTypes[0]), gen.UserTypeJS(elemTypes[1]))
	case types.PackageKind:
		return gen.ImportJSType("Package")
	case types.ValueKind:
		return gen.ImportJSType("Value")
	case types.TypeKind:
		return gen.ImportJSType("Type")
	}
	panic("unreachable")
}

// DefToValue returns a string containing Go code to convert an instance of a Def type (named val) to a Noms types.Value of the type described by t.
func (gen *Generator) DefToValue(val string, t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	switch rt.Kind() {
	case types.BlobKind, types.EnumKind, types.PackageKind, types.ValueKind, types.TypeKind:
		return val // No special Def representation
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return gen.NativeToValue(val, rt)
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.New()", val)
	case types.RefKind:
		return fmt.Sprintf("New%s(%s)", gen.UserName(rt), val)
	}
	panic("unreachable")
}

// DefToUser returns a string containing Go code to convert an instance of a Def type (named val) to a User type described by t.
func (gen *Generator) DefToUser(val string, t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	switch rt.Kind() {
	case types.BlobKind, types.BoolKind, types.EnumKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.PackageKind, types.StringKind, types.TypeKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind, types.ValueKind:
		return val
	case types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return gen.DefToValue(val, rt)
	}
	panic("unreachable")
}

// MayHaveChunks returns whether the type (t) may contain more chunks.
func (gen *Generator) MayHaveChunks(t types.Type) bool {
	rt := gen.R.Resolve(t, gen.Package)
	switch rt.Kind() {
	case types.BlobKind, types.ListKind, types.MapKind, types.PackageKind, types.RefKind, types.SetKind, types.StructKind, types.TypeKind, types.ValueKind:
		return true
	case types.BoolKind, types.EnumKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return false
	}
	panic("unreachable")
}

// ValueToDef returns a string containing Go code to convert an instance of a types.Value (val) into the Def type appropriate for t.
func (gen *Generator) ValueToDef(val string, t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	switch rt.Kind() {
	case types.BlobKind, types.PackageKind, types.TypeKind:
		return gen.ValueToUser(val, rt) // No special Def representation
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return gen.ValueToNative(val, rt)
	case types.EnumKind:
		return fmt.Sprintf("%s.(%s)", val, gen.UserName(t))
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.Def()", gen.ValueToUser(val, t))
	case types.RefKind:
		return fmt.Sprintf("%s.TargetRef()", gen.ValueToUser(val, t))
	case types.ValueKind:
		return val // Value is already a Value
	}
	panic("unreachable")
}

// UserToDef returns a string containing Go code to convert an User value (val) into the Def type appropriate for t.
func (gen *Generator) UserToDef(val string, t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	switch rt.Kind() {
	case types.BlobKind, types.EnumKind, types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.PackageKind, types.StringKind, types.TypeKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind, types.ValueKind:
		return val
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.Def()", val)
	case types.RefKind:
		return fmt.Sprintf("%s.TargetRef()", val)
	}
	panic("unreachable")
}

// NativeToValue returns a string containing Go code to convert an instance of a native type (named val) to a Noms types.Value of the type described by t.
func (gen *Generator) NativeToValue(val string, t types.Type) string {
	t = gen.R.Resolve(t, gen.Package)
	k := t.Kind()
	switch k {
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return fmt.Sprintf("%s%s(%s)", gen.TypesPackage, kindToString(k), val)
	case types.StringKind:
		return fmt.Sprintf("%sNewString(%s)", gen.TypesPackage, val)
	}
	panic("unreachable")
}

// ValueToNative returns a string containing Go code to convert an instance of a types.Value (val) into the native type appropriate for t.
func (gen *Generator) ValueToNative(val string, t types.Type) string {
	k := t.Kind()
	switch k {
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		n := kindToString(k)
		return fmt.Sprintf("%s(%s.(%s%s))", strings.ToLower(n), val, gen.TypesPackage, n)
	case types.StringKind:
		return fmt.Sprintf("%s.(%sString).String()", val, gen.TypesPackage)
	}
	panic("unreachable")
}

// UserToValue returns a string containing Go code to convert an instance of a User type (named val) to a Noms types.Value of the type described by t. For Go primitive types, this will use NativeToValue(). For other types, their UserType is a Noms types.Value (or a wrapper around one), so this is more-or-less a pass-through.
func (gen *Generator) UserToValue(val string, t types.Type) string {
	t = gen.R.Resolve(t, gen.Package)
	k := t.Kind()
	switch k {
	case types.BlobKind, types.EnumKind, types.ListKind, types.MapKind, types.PackageKind, types.RefKind, types.SetKind, types.StructKind, types.TypeKind, types.ValueKind:
		return val
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return gen.NativeToValue(val, t)
	}
	panic("unreachable")
}

// ValueToUser returns a string containing Go code to convert an instance of a types.Value (val) into the User type appropriate for t. For Go primitives, this will use ValueToNative().
func (gen *Generator) ValueToUser(val string, t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%s.(%sBlob)", val, gen.TypesPackage)
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return gen.ValueToNative(val, rt)
	case types.EnumKind, types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.(%s)", val, gen.UserName(t))
	case types.PackageKind:
		return fmt.Sprintf("%s.(%sPackage)", val, gen.TypesPackage)
	case types.ValueKind:
		return val
	case types.TypeKind:
		return fmt.Sprintf("%s.(%sType)", val, gen.TypesPackage)
	}
	panic("unreachable")
}

// UserZero returns a string containing Go code to create an uninitialized instance of the User type appropriate for t.
func (gen *Generator) UserZero(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%sNewEmptyBlob()", gen.TypesPackage)
	case types.BoolKind:
		return "false"
	case types.EnumKind:
		return fmt.Sprintf("New%s()", gen.UserName(rt))
	case types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return fmt.Sprintf("%s(0)", strings.ToLower(kindToString(k)))
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("New%s()", gen.UserName(rt))
	case types.PackageKind:
		return fmt.Sprintf("New%s()", gen.UserName(rt))
	case types.RefKind:
		return fmt.Sprintf("New%s(ref.Ref{})", gen.UserName(rt))
	case types.StringKind:
		return `""`
	case types.ValueKind:
		// TODO: This is where a null Value would have been useful.
		return fmt.Sprintf("%sBool(false)", gen.TypesPackage)
	case types.TypeKind:
		return fmt.Sprintf("%sType{R: ref.Ref{}}", gen.TypesPackage)
	}
	panic("unreachable")
}

// ValueZero returns a string containing Go code to create an uninitialized instance of the Noms types.Value appropriate for t.
func (gen *Generator) ValueZero(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%sNewEmptyBlob()", gen.TypesPackage)
	case types.BoolKind:
		return fmt.Sprintf("%sBool(false)", gen.TypesPackage)
	case types.EnumKind:
		return gen.UserZero(t)
	case types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind:
		return fmt.Sprintf("%s%s(0)", gen.TypesPackage, kindToString(k))
	case types.ListKind, types.MapKind, types.RefKind, types.SetKind:
		return gen.UserZero(t)
	case types.PackageKind:
		return fmt.Sprintf("%sNewPackage()", gen.TypesPackage)
	case types.StringKind:
		return fmt.Sprintf(`%sNewString("")`, gen.TypesPackage)
	case types.StructKind:
		return fmt.Sprintf("New%s()", gen.UserName(rt))
	case types.ValueKind:
		// TODO: Use nil here
		return fmt.Sprintf("%sBool(false)", gen.TypesPackage)
	case types.TypeKind:
		return fmt.Sprintf("%sType{R: ref.Ref{}}", gen.TypesPackage)
	}
	panic("unreachable")
}

// UserName returns the name of the User type appropriate for t, taking into account Noms types imported from other packages.
func (gen *Generator) UserName(t types.Type) string {
	rt := gen.R.Resolve(t, gen.Package)
	k := rt.Kind()
	switch k {
	case types.BlobKind, types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.PackageKind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind, types.ValueKind, types.TypeKind:
		return kindToString(k)
	case types.EnumKind:
		return rt.Name()
	case types.ListKind:
		return fmt.Sprintf("ListOf%s", gen.refToID(rt.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.MapKind:
		elemTypes := rt.Desc.(types.CompoundDesc).ElemTypes
		return fmt.Sprintf("MapOf%sTo%s", gen.refToID(elemTypes[0]), gen.refToID(elemTypes[1]))
	case types.RefKind:
		return fmt.Sprintf("RefOf%s", gen.refToID(rt.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.SetKind:
		return fmt.Sprintf("SetOf%s", gen.refToID(rt.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.StructKind:
		// We get an empty name when we have a struct that is used as union
		if rt.Name() == "" {
			choices := rt.Desc.(types.StructDesc).Union
			s := "__unionOf"
			for i, f := range choices {
				if i > 0 {
					s += "And"
				}
				s += strings.Title(f.Name) + "Of" + gen.refToID(f.T)
			}
			return s
		}
		return rt.Name()
	}
	panic("unreachable")
}

func (gen Generator) importedUserNameJS(t types.Type) string {
	d.Chk.True(t.HasPackageRef())
	return fmt.Sprintf("%s.%s", gen.RefToAliasName(t.PackageRef()), gen.UserName(t))
}

func (gen *Generator) refToID(t types.Type) string {
	if !t.IsUnresolved() || !t.HasPackageRef() {
		return gen.UserName(t)
	}
	return gen.UserName(gen.R.Resolve(t, gen.Package))
}

// RefToJSIdentfierName generates an identifier name representing a Ref. ie. `sha1_abc1234`.
func (gen *Generator) RefToJSIdentfierName(r ref.Ref) string {
	return strings.Replace(r.String(), "-", "_", 1)[0:12]
}

// RefToAliasName is used to map the ref of an import to the alias name used in the noms file
func (gen *Generator) RefToAliasName(r ref.Ref) string {
	// When we generate code from a Package stored in a DataStore we do not have the alias names.
	if n, ok := gen.AliasNames[r]; ok {
		return n
	}
	return fmt.Sprintf("_%s", gen.RefToJSIdentfierName(r))
}

// ToTypesType returns a string containing Go code that instantiates a types.Type instance equivalent to t.
func (gen *Generator) ToTypesType(t types.Type, inPackageDef bool) string {
	if t.IsUnresolved() {
		d.Chk.True(t.HasPackageRef())
		d.Chk.True(t.HasOrdinal(), "%s does not have an ordinal set", t.Name())
		if t.PackageRef() == gen.Package.Ref() && inPackageDef {
			return fmt.Sprintf(`%sMakeType(ref.Ref{}, %d)`, gen.TypesPackage, t.Ordinal())
		}
		return fmt.Sprintf(`%sMakeType(ref.Parse("%s"), %d)`, gen.TypesPackage, t.PackageRef().String(), t.Ordinal())
	}

	if types.IsPrimitiveKind(t.Kind()) {
		return fmt.Sprintf("%sMakePrimitiveType(%s%sKind)", gen.TypesPackage, gen.TypesPackage, kindToString(t.Kind()))
	}

	switch desc := t.Desc.(type) {
	case types.CompoundDesc:
		types := make([]string, len(desc.ElemTypes))
		for i, t := range desc.ElemTypes {
			types[i] = gen.ToTypesType(t, inPackageDef)
		}
		return fmt.Sprintf(`%sMakeCompoundType(%s%sKind, %s)`, gen.TypesPackage, gen.TypesPackage, kindToString(t.Kind()), strings.Join(types, ", "))
	case types.EnumDesc:
		return fmt.Sprintf(`%sMakeEnumType("%s", "%s")`, gen.TypesPackage, t.Name(), strings.Join(desc.IDs, `", "`))
	case types.StructDesc:
		flatten := func(f []types.Field) string {
			out := make([]string, 0, len(f))
			for _, field := range f {
				out = append(out, fmt.Sprintf(`%sField{"%s", %s, %t},`, gen.TypesPackage, field.Name, gen.ToTypesType(field.T, inPackageDef), field.Optional))
			}
			return strings.Join(out, "\n")
		}
		fields := fmt.Sprintf("[]%sField{\n%s\n}", gen.TypesPackage, flatten(desc.Fields))
		choices := fmt.Sprintf("%sChoices{\n%s\n}", gen.TypesPackage, flatten(desc.Union))
		return fmt.Sprintf("%sMakeStructType(\"%s\",\n%s,\n%s,\n)", gen.TypesPackage, t.Name(), fields, choices)
	default:
		d.Chk.Fail("Unknown TypeDesc.", "%#v (%T)", desc, desc)
	}
	panic("Unreachable")
}

func ind(i int) string {
	return strings.Repeat("  ", i)
}

func firstToLower(s string) string {
	b := []rune(s)
	b[0] = unicode.ToLower(b[0])
	return string(b)
}

// ToTypeValueJS returns a string containing JS code that instantiates a Type instance equivalent to t for JavaScript.
func (gen *Generator) ToTypeValueJS(t types.Type, inPackageDef bool, indent int) string {
	if t.IsUnresolved() {
		d.Chk.True(t.HasPackageRef())
		d.Chk.True(t.HasOrdinal(), "%s does not have an ordinal set", t.Name())
		if t.PackageRef() == gen.Package.Ref() {
			if inPackageDef {
				return fmt.Sprintf(`%s(%s, %d)`, gen.ImportJS("makeType"), gen.ImportJS("emptyRef"), t.Ordinal())
			} else {
				return fmt.Sprintf(`%s(_pkg.ref, %d)`, gen.ImportJS("makeType"), t.Ordinal())
			}
		}
		return fmt.Sprintf(`%s(%s.parse('%s'), %d)`, gen.ImportJS("makeType"), gen.ImportJS("Ref"), t.PackageRef().String(), t.Ordinal())
	}

	if types.IsPrimitiveKind(t.Kind()) {
		return gen.ImportJS(firstToLower(kindToString(t.Kind())) + "Type")
	}

	switch desc := t.Desc.(type) {
	case types.CompoundDesc:
		types := make([]string, len(desc.ElemTypes))
		for i, t := range desc.ElemTypes {
			types[i] = gen.ToTypeValueJS(t, inPackageDef, 0)
		}
		return fmt.Sprintf(`%s(%s.%s, %s)`, gen.ImportJS("makeCompoundType"), gen.ImportJS("Kind"), kindToString(t.Kind()), strings.Join(types, ", "))
	case types.EnumDesc:
		return fmt.Sprintf(`%s('%s', '%s')`, gen.ImportJS("makeEnumType"), t.Name(), strings.Join(desc.IDs, `', '`))
	case types.StructDesc:
		flatten := func(f []types.Field) string {
			out := make([]string, 0, len(f))
			for _, field := range f {
				out = append(out, fmt.Sprintf(`%snew %s('%s', %s, %t),`, ind(indent+1), gen.ImportJS("Field"), field.Name, gen.ToTypeValueJS(field.T, inPackageDef, 0), field.Optional))
			}
			return strings.Join(out, "\n")
		}
		fields := fmt.Sprintf("%s[\n%s\n%s]", ind(indent), flatten(desc.Fields), ind(indent))
		choices := fmt.Sprintf("%s[\n%s\n%s]", ind(indent), flatten(desc.Union), ind(indent))
		return fmt.Sprintf("%s('%s',\n%s,\n%s\n%s)", gen.ImportJS("makeStructType"), t.Name(), fields, choices, ind(indent-1))
	default:
		d.Chk.Fail("Unknown TypeDesc.", "%#v (%T)", desc, desc)
	}
	panic("Unreachable")
}

// IsLast determines if |index| is the last index in |seq|.
func (gen *Generator) IsLast(index int, seq interface{}) bool {
	return reflect.ValueOf(seq).Len() == index+1
}

// ToTag replaces "-" characters in s with "_", so it can be used in a Go identifier.
// TODO: replace other illegal chars as well?
func ToTag(r ref.Ref) string {
	return strings.Replace(r.String()[0:12], "-", "_", -1)
}

func kindToString(k types.NomsKind) (out string) {
	out = types.KindToString[k]
	d.Chk.NotEmpty(out, "Unknown NomsKind %d", k)
	return
}

// ImportJS returns the name of the imported binding as well as registers the binding as imported so that we can later generate the right import declaration.
func (gen *Generator) ImportJS(name string) string {
	if gen.ImportedJS == nil {
		gen.ImportedJS = map[string]bool{}
	}
	gen.ImportedJS[name] = true
	return fmt.Sprintf("_%s", name)
}

// ImportJSType returns the name of the imported type as well as registers the type as imported so that we can later generate the right import type declaration.
func (gen *Generator) ImportJSType(name string) string {
	if gen.ImportedJSTypes == nil {
		gen.ImportedJSTypes = map[string]bool{}
	}
	gen.ImportedJSTypes[name] = true
	return fmt.Sprintf("_%s", name)
}
