// Package code provides Generator, which has methods for generating code snippets from a types.TypeRef.
// Conceptually there are few type spaces here:
//
// - Def - MyStructDef, ListOfBoolDef; convenient Go types for working with data from a given Noms Value.
// - Native - such as string, uint32
// - Value - the generic types.Value
// - Nom - types.String, types.UInt32, MyStruct, ListOfBool
// - User - User defined structs, enums etc as well as native primitves. This uses Native when possible or Nom if not. These are to be used in APIs for generated types -- Getters and setters for maps and structs, etc.
package code

import (
	"fmt"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

// Resolver provides a single method for resolving an unresolved types.TypeRef.
type Resolver interface {
	Resolve(t types.TypeRef) types.TypeRef
}

// Generator provides methods for generating code snippets from both resolved and unresolved types.TypeRefs. In the latter case, it uses R to resolve the types.TypeRef before generating code.
type Generator struct {
	R Resolver
}

// DefType returns a string containing the Go type that should be used as the 'Def' for the Noms type described by t.
func (gen Generator) DefType(t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind:
		return "types.Blob"
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return strings.ToLower(kindToString(k))
	case types.EnumKind:
		return gen.UserName(t)
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return gen.UserName(t) + "Def"
	case types.RefKind:
		return "ref.Ref"
	case types.ValueKind:
		return "types.Value"
	case types.TypeRefKind:
		return "types.TypeRef"
	}
	panic("unreachable")
}

// UserType returns a string containing the Go type that should be used when the Noms type described by t needs to be returned by a generated getter or taken as a parameter to a generated setter.
func (gen Generator) UserType(t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind:
		return "types.Blob"
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return strings.ToLower(kindToString(k))
	case types.EnumKind, types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return gen.UserName(t)
	case types.ValueKind:
		return "types.Value"
	case types.TypeRefKind:
		return "types.TypeRef"
	}
	panic("unreachable")
}

// DefToValue returns a string containing Go code to convert an instance of a Def type (named val) to a Noms types.Value of the type described by t.
func (gen Generator) DefToValue(val string, t types.TypeRef) string {
	t = gen.R.Resolve(t)
	switch t.Desc.Kind() {
	case types.BlobKind, types.ValueKind, types.TypeRefKind:
		return val // Blob & Value type has no Def
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return gen.NativeToValue(val, t)
	case types.EnumKind:
		return fmt.Sprintf("types.UInt32(%s)", val)
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.New().NomsValue()", val)
	case types.RefKind:
		return fmt.Sprintf("types.Ref{R: %s}", val)
	}
	panic("unreachable")
}

// ValueToDef returns a string containing Go code to convert an instance of a types.Value (val) into the Def type appropriate for t.
func (gen Generator) ValueToDef(val string, t types.TypeRef) string {
	t = gen.R.Resolve(t)
	switch t.Desc.Kind() {
	case types.BlobKind:
		return gen.ValueToUser(val, t)
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return gen.ValueToNative(val, t)
	case types.EnumKind:
		return fmt.Sprintf("%s(%s.(types.UInt32))", gen.UserName(t), val)
	case types.ListKind, types.MapKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.Def()", gen.ValueToUser(val, t))
	case types.RefKind:
		return fmt.Sprintf("%s.Ref()", val)
	case types.ValueKind:
		return val // Value type has no Def
	case types.TypeRefKind:
		return gen.ValueToUser(val, t)
	}
	panic("unreachable")
}

// NativeToValue returns a string containing Go code to convert an instance of a native type (named val) to a Noms types.Value of the type described by t.
func (gen Generator) NativeToValue(val string, t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return fmt.Sprintf("types.%s(%s)", kindToString(k), val)
	case types.EnumKind:
		return fmt.Sprintf("types.UInt32(%s)", val)
	case types.StringKind:
		return "types.NewString(" + val + ")"
	}
	panic("unreachable")
}

// ValueToNative returns a string containing Go code to convert an instance of a types.Value (val) into the native type appropriate for t.
func (gen Generator) ValueToNative(val string, t types.TypeRef) string {
	k := t.Desc.Kind()
	switch k {
	case types.EnumKind:
		return fmt.Sprintf("%s(%s.(types.UInt32))", gen.UserType(t), val)
	case types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		n := kindToString(k)
		return fmt.Sprintf("%s(%s.(types.%s))", strings.ToLower(n), val, n)
	case types.StringKind:
		return val + ".(types.String).String()"
	}
	panic("unreachable")
}

// UserToValue returns a string containing Go code to convert an instance of a User type (named val) to a Noms types.Value of the type described by t. For Go primitive types, this will use NativeToValue(). For other types, their UserType is a Noms types.Value (or a wrapper around one), so this is more-or-less a pass-through.
func (gen Generator) UserToValue(val string, t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind, types.ValueKind, types.TypeRefKind:
		return val
	case types.BoolKind, types.EnumKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return gen.NativeToValue(val, t)
	case types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%s.NomsValue()", val)
	}
	panic("unreachable")
}

// ValueToUser returns a string containing Go code to convert an instance of a types.Value (val) into the User type appropriate for t. For Go primitives, this will use ValueToNative().
func (gen Generator) ValueToUser(val string, t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind:
		return fmt.Sprintf("%s.(types.Blob)", val)
	case types.BoolKind, types.EnumKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return gen.ValueToNative(val, t)
	case types.ListKind, types.MapKind, types.RefKind, types.SetKind, types.StructKind:
		return fmt.Sprintf("%sFromVal(%s)", gen.UserName(t), val)
	case types.ValueKind:
		return val
	case types.TypeRefKind:
		return fmt.Sprintf("%s.(types.TypeRef)", val)
	}
	panic("unreachable")
}

// UserZero returns a string containing Go code to create an uninitialized instance of the User type appropriate for t.
func (gen Generator) UserZero(t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind:
		return "types.NewEmptyBlob()"
	case types.BoolKind:
		return "false"
	case types.EnumKind:
		return fmt.Sprintf("%s(0)", gen.UserType(t))
	case types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return fmt.Sprintf("%s(0)", strings.ToLower(kindToString(k)))
	case types.ListKind, types.MapKind, types.SetKind:
		return fmt.Sprintf("New%s()", gen.UserType(t))
	case types.RefKind:
		return fmt.Sprintf("%s{ref.Ref{}}", gen.UserType(t))
	case types.StringKind:
		return `""`
	case types.StructKind:
		return fmt.Sprintf("New%s()", gen.UserName(t))
	case types.ValueKind:
		// TODO: This is where a null Value would have been useful.
		return "types.Bool(false)"
	case types.TypeRefKind:
		return "types.TypeRef{}"
	}
	panic("unreachable")
}

// ValueZero returns a string containing Go code to create an uninitialized instance of the Noms types.Value appropriate for t.
func (gen Generator) ValueZero(t types.TypeRef) string {
	t = gen.R.Resolve(t)
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind:
		return "types.NewEmptyBlob()"
	case types.BoolKind:
		return "types.Bool(false)"
	case types.EnumKind:
		return "types.UInt32(0)"
	case types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind:
		return fmt.Sprintf("types.%s(0)", kindToString(k))
	case types.ListKind:
		return "types.NewList()"
	case types.MapKind:
		return "types.NewMap()"
	case types.RefKind:
		return "types.Ref{R: ref.Ref{}}"
	case types.SetKind:
		return "types.NewSet()"
	case types.StringKind:
		return `types.NewString("")`
	case types.StructKind:
		components := gen.userNameComponents(t)
		d.Chk.True(len(components) == 1 || len(components) == 2)
		if len(components) == 2 {
			return fmt.Sprintf("%s.New%s().NomsValue()", components[0], components[1])
		}
		return fmt.Sprintf("New%s().NomsValue()", components[0])
	case types.ValueKind:
		// TODO: This is where a null Value would have been useful.
		return "types.Bool(false)"
	case types.TypeRefKind:
		return "types.TypeRef{}"
	}
	panic("unreachable")
}

// UserName returns the name of the User type appropriate for t, taking into account Noms types imported from other packages.
func (gen Generator) UserName(t types.TypeRef) string {
	t = gen.R.Resolve(t)
	toID := func(t types.TypeRef) string {
		return strings.Join(gen.userNameComponents(t), "_")
	}
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind, types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind, types.ValueKind, types.TypeRefKind:
		return kindToString(k)
	case types.EnumKind:
		if t.HasPackageRef() {
			return ToTag(t.PackageRef().String()) + "." + t.Name()
		}
		return t.Name()
	case types.ListKind:
		return fmt.Sprintf("ListOf%s", toID(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.MapKind:
		elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
		return fmt.Sprintf("MapOf%sTo%s", toID(elemTypes[0]), toID(elemTypes[1]))
	case types.RefKind:
		return fmt.Sprintf("RefOf%s", toID(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.SetKind:
		return fmt.Sprintf("SetOf%s", toID(t.Desc.(types.CompoundDesc).ElemTypes[0]))
	case types.StructKind:
		// We get an empty name when we have a struct that is used as union
		if t.Name() == "" {
			choices := t.Desc.(types.StructDesc).Union
			s := "__unionOf"
			for i, f := range choices {
				if i > 0 {
					s += "And"
				}
				s += strings.Title(f.Name) + "Of" + toID(f.T)
			}
			return s
		}
		if t.HasPackageRef() {
			return ToTag(t.PackageRef().String()) + "." + t.Name()
		}
		return t.Name()
	}
	panic("unreachable")
}

func (gen Generator) userNameComponents(t types.TypeRef) []string {
	userName := gen.UserName(t)
	if period := strings.LastIndex(userName, "."); period != -1 {
		return []string{userName[:period], userName[period+1:]}
	}
	return []string{userName}
}

// ToTypeRef returns a string containing Go code that instantiates a types.TypeRef instance equivalent to t.
func (gen Generator) ToTypeRef(t types.TypeRef, fileID, packageName string) string {
	if t.HasPackageRef() {
		return fmt.Sprintf(`types.MakeTypeRef("%s", ref.Parse("%s"))`, t.Name(), t.PackageRef().String())
	}
	if t.IsUnresolved() && fileID != "" {
		return fmt.Sprintf(`types.MakeTypeRef("%s", __%sPackageInFile_%s_CachedRef)`, t.Name(), packageName, fileID)
	}
	if t.IsUnresolved() {
		return fmt.Sprintf(`types.MakeTypeRef("%s", ref.Ref{})`, t.Name())
	}

	if types.IsPrimitiveKind(t.Desc.Kind()) {
		return fmt.Sprintf("types.MakePrimitiveTypeRef(types.%sKind)", kindToString(t.Desc.Kind()))
	}
	switch desc := t.Desc.(type) {
	case types.CompoundDesc:
		typerefs := make([]string, len(desc.ElemTypes))
		for i, t := range desc.ElemTypes {
			typerefs[i] = gen.ToTypeRef(t, fileID, packageName)
		}
		return fmt.Sprintf(`types.MakeCompoundTypeRef("%s", types.%sKind, %s)`, t.Name(), kindToString(t.Desc.Kind()), strings.Join(typerefs, ", "))
	case types.EnumDesc:
		return fmt.Sprintf(`types.MakeEnumTypeRef("%s", "%s")`, t.Name(), strings.Join(desc.IDs, `", "`))
	case types.StructDesc:
		flatten := func(f []types.Field) string {
			out := make([]string, 0, len(f))
			for _, field := range f {
				out = append(out, fmt.Sprintf(`types.Field{"%s", %s, %t},`, field.Name, gen.ToTypeRef(field.T, fileID, packageName), field.Optional))
			}
			return strings.Join(out, "\n")
		}
		fields := fmt.Sprintf("[]types.Field{\n%s\n}", flatten(desc.Fields))
		choices := fmt.Sprintf("types.Choices{\n%s\n}", flatten(desc.Union))
		return fmt.Sprintf("types.MakeStructTypeRef(\"%s\",\n%s,\n%s,\n)", t.Name(), fields, choices)
	default:
		d.Chk.Fail("Unknown TypeDesc.", "%#v (%T)", desc, desc)
	}
	panic("Unreachable")
}

// ToTag replaces "-" characters in s with "_", so it can be used in a Go identifier.
// TODO: replace other illegal chars as well?
func ToTag(s string) string {
	return strings.Replace(s, "-", "_", -1)
}

func kindToString(k types.NomsKind) (out string) {
	out = types.KindToString[k]
	d.Chk.NotEmpty(out, "Unknown NomsKind %d", k)
	return
}
