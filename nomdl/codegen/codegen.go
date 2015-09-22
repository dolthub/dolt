package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"text/template"

	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/tools/imports"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/nomdl/parse"
)

var (
	inFlag      = flag.String("in", "", "The name of the noms file to read")
	outFlag     = flag.String("out", "", "The name of the go file to write")
	packageFlag = flag.String("package", "", "The name of the go package to write")
)

const ext = ".noms"

func main() {
	flag.Parse()

	packageName := getGoPackageName()
	if *inFlag != "" {
		out := *outFlag
		if out == "" {
			out = getOutFileName(*inFlag)
		}
		generate(packageName, *inFlag, out)
		return
	}

	// Generate code from all .noms file in the current directory
	nomsFiles, err := filepath.Glob("*" + ext)
	d.Chk.NoError(err)
	for _, n := range nomsFiles {
		generate(packageName, n, getOutFileName(n))
	}
}

func getOutFileName(in string) string {
	return in[:len(in)-len(ext)] + ".go"
}

func generate(packageName, in, out string) {
	inFile, err := os.Open(in)
	d.Chk.NoError(err)
	defer inFile.Close()

	var buf bytes.Buffer
	pkg := parse.ParsePackage("", inFile)
	gen := NewCodeGen(&buf, pkg)
	gen.WritePackage(packageName)

	bs, err := imports.Process(out, buf.Bytes(), nil)
	d.Chk.NoError(err)

	outFile, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	d.Chk.NoError(err)
	defer outFile.Close()

	io.Copy(outFile, bytes.NewBuffer(bs))
}

func getGoPackageName() string {
	if *packageFlag != "" {
		return *packageFlag
	}

	// It is illegal to have multiple go files in the same directory with different package names.
	// We can therefore just pick the first one and get the package name from there.
	goFiles, err := filepath.Glob("*.go")
	d.Chk.NoError(err)
	d.Chk.True(len(goFiles) > 0)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, goFiles[0], nil, parser.PackageClauseOnly)
	d.Chk.NoError(err)
	return f.Name.String()
}

type codeGen struct {
	w         io.Writer
	pkg       parse.Package
	written   map[string]bool
	templates *template.Template
}

func NewCodeGen(w io.Writer, pkg parse.Package) *codeGen {
	gen := &codeGen{w, pkg, map[string]bool{}, nil}
	gen.templates = gen.readTemplates()
	return gen
}

func (gen *codeGen) readTemplates() *template.Template {
	_, thisfile, _, _ := runtime.Caller(1)
	glob := path.Join(path.Dir(thisfile), "*.tmpl")
	return template.Must(template.New("").Funcs(
		template.FuncMap{
			"defType":     gen.defType,
			"defToValue":  gen.defToValue,
			"valueToDef":  gen.valueToDef,
			"userType":    gen.userType,
			"userToValue": gen.userToValue,
			"valueToUser": gen.valueToUser,
			"userZero":    gen.userZero,
			"valueZero":   gen.valueZero,
			"title":       strings.Title,
		}).ParseGlob(glob))
}

// Conceptually there are few type spaces here:
//
// - Def - MyStructDef, ListOfBoolDef
// - Native - such as string, uint32
// - Value - the generic types.Value
// - Nom - types.String, types.UInt32, MyStruct, ListOfBool
// - User - User defined structs, enums etc as well as native primitves. This uses Native when possible or Nom if not
//
// These naming conventions are used for the conversion functions available
// in the templates.

func (gen *codeGen) defType(t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind:
		return "types.Blob"
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return strings.ToLower(primitiveKindToString(k))
	case parse.EnumKind:
		return gen.userName(t)
	case parse.ListKind, parse.MapKind, parse.SetKind, parse.StructKind:
		return gen.userName(t) + "Def"
	case parse.RefKind:
		return "ref.Ref"
	case parse.ValueKind:
		return "types.Value"
	}
	panic("unreachable")
}

func (gen *codeGen) userType(t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind:
		return "types.Blob"
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return strings.ToLower(primitiveKindToString(k))
	case parse.EnumKind, parse.ListKind, parse.MapKind, parse.RefKind, parse.SetKind, parse.StructKind:
		return gen.userName(t)
	case parse.ValueKind:
		return "types.Value"
	}
	panic("unreachable")
}

func (gen *codeGen) defToValue(val string, t parse.TypeRef) string {
	t = gen.resolve(t)
	switch t.Desc.Kind() {
	case parse.BlobKind, parse.ValueKind:
		return val // Blob & Value type has no Def
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return gen.nativeToValue(val, t)
	case parse.EnumKind:
		return fmt.Sprintf("types.Int32(%s)", val)
	case parse.ListKind, parse.MapKind, parse.SetKind, parse.StructKind:
		return fmt.Sprintf("%s.New().NomsValue()", val)
	case parse.RefKind:
		return fmt.Sprintf("types.Ref{R: %s}", val)
	}
	panic("unreachable")
}

func (gen *codeGen) valueToDef(val string, t parse.TypeRef) string {
	t = gen.resolve(t)
	switch t.Desc.Kind() {
	case parse.BlobKind:
		return gen.valueToUser(val, t)
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return gen.valueToNative(val, t)
	case parse.EnumKind:
		return fmt.Sprintf("%s(%s.(types.Int32))", gen.userName(t), val)
	case parse.ListKind, parse.MapKind, parse.SetKind, parse.StructKind:
		return fmt.Sprintf("%s.Def()", gen.valueToUser(val, t))
	case parse.RefKind:
		return fmt.Sprintf("%s.Ref()", val)
	case parse.ValueKind:
		return val // Value type has no Def
	}
	panic("unreachable")
}

func primitiveKindToString(k parse.NomsKind) string {
	switch k {
	case parse.BlobKind:
		return "Blob"
	case parse.BoolKind:
		return "Bool"
	case parse.Float32Kind:
		return "Float32"
	case parse.Float64Kind:
		return "Float64"
	case parse.Int16Kind:
		return "Int16"
	case parse.Int32Kind:
		return "Int32"
	case parse.Int64Kind:
		return "Int64"
	case parse.Int8Kind:
		return "Int8"
	case parse.StringKind:
		return "String"
	case parse.UInt16Kind:
		return "UInt16"
	case parse.UInt32Kind:
		return "UInt32"
	case parse.UInt64Kind:
		return "UInt64"
	case parse.UInt8Kind:
		return "UInt8"
	case parse.ValueKind:
		return "Value"
	}
	panic("unreachable")
}

func (gen *codeGen) nativeToValue(val string, t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return fmt.Sprintf("types.%s(%s)", primitiveKindToString(k), val)
	case parse.EnumKind:
		return fmt.Sprintf("types.Int32(%s)", val)
	case parse.StringKind:
		return "types.NewString(" + val + ")"
	}
	panic("unreachable")
}

func (gen *codeGen) valueToNative(val string, t parse.TypeRef) string {
	k := t.Desc.Kind()
	switch k {
	case parse.EnumKind:
		return fmt.Sprintf("%s(%s.(types.Int32))", gen.userType(t), val)
	case parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		n := primitiveKindToString(k)
		return fmt.Sprintf("%s(%s.(types.%s))", strings.ToLower(n), val, n)
	case parse.StringKind:
		return val + ".(types.String).String()"
	}
	panic("unreachable")
}

func (gen *codeGen) userToValue(val string, t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind, parse.ValueKind:
		return val
	case parse.BoolKind, parse.EnumKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return gen.nativeToValue(val, t)
	case parse.ListKind, parse.MapKind, parse.RefKind, parse.SetKind, parse.StructKind:
		return fmt.Sprintf("%s.NomsValue()", val)
	}
	panic("unreachable")
}

func (gen *codeGen) valueToUser(val string, t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind:
		return fmt.Sprintf("%s.(types.Blob)", val)
	case parse.BoolKind, parse.EnumKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return gen.valueToNative(val, t)
	case parse.ListKind, parse.MapKind, parse.RefKind, parse.SetKind, parse.StructKind:
		return fmt.Sprintf("%sFromVal(%s)", gen.userName(t), val)
	case parse.ValueKind:
		return val
	}
	panic("unreachable")
}

func (gen *codeGen) userZero(t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind:
		return "types.NewEmptyBlob()"
	case parse.BoolKind:
		return "false"
	case parse.EnumKind:
		return fmt.Sprintf("%s(0)", gen.userType(t))
	case parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return fmt.Sprintf("%s(0)", strings.ToLower(primitiveKindToString(k)))
	case parse.ListKind, parse.MapKind, parse.SetKind:
		return fmt.Sprintf("New%s()", gen.userType(t))
	case parse.RefKind:
		return fmt.Sprintf("%s{ref.Ref{}}", gen.userType(t))
	case parse.StringKind:
		return `""`
	case parse.ValueKind:
		// TODO: This is where a null Value would have been useful.
		return "types.Bool(false)"
	}
	panic("unreachable")
}

func (gen *codeGen) valueZero(t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind:
		return "types.NewEmptyBlob()"
	case parse.BoolKind:
		return "types.Bool(false)"
	case parse.EnumKind:
		return "types.Int32(0)"
	case parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind:
		return fmt.Sprintf("types.%s(0)", primitiveKindToString(k))
	case parse.ListKind:
		return "types.NewList()"
	case parse.MapKind:
		return "types.NewMap()"
	case parse.RefKind:
		return "types.Ref{R: ref.Ref{}}"
	case parse.SetKind:
		return "types.NewSet()"
	case parse.StringKind:
		return `types.NewString("")`
	case parse.StructKind:
		return fmt.Sprintf("New%s().NomsValue()", gen.userName(t))
	case parse.ValueKind:
		// TODO: This is where a null Value would have been useful.
		return "types.Bool(false)"
	}
	panic("unreachable")
}

func (gen *codeGen) userName(t parse.TypeRef) string {
	t = gen.resolve(t)
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind, parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind, parse.ValueKind:
		return primitiveKindToString(k)
	case parse.EnumKind:
		return t.Name
	case parse.ListKind:
		return fmt.Sprintf("ListOf%s", gen.userName(t.Desc.(parse.CompoundDesc).ElemTypes[0]))
	case parse.MapKind:
		elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
		return fmt.Sprintf("MapOf%sTo%s", gen.userName(elemTypes[0]), gen.userName(elemTypes[1]))
	case parse.RefKind:
		return fmt.Sprintf("RefOf%s", gen.userName(t.Desc.(parse.CompoundDesc).ElemTypes[0]))
	case parse.SetKind:
		return fmt.Sprintf("SetOf%s", gen.userName(t.Desc.(parse.CompoundDesc).ElemTypes[0]))
	case parse.StructKind:
		// We get an empty name when we have a struct that is used as union
		if t.Name == "" {
			union := t.Desc.(parse.StructDesc).Union
			s := "__unionOf"
			for i, f := range union.Choices {
				if i > 0 {
					s += "And"
				}
				s += strings.Title(f.Name) + "Of" + gen.userName(f.T)
			}
			return s
		}
		return t.Name
	}
	panic("unreachable")
}

func (gen *codeGen) resolve(t parse.TypeRef) parse.TypeRef {
	if !t.IsUnresolved() {
		return t
	}
	return gen.pkg.NamedTypes[t.Name]
}

func (gen *codeGen) WritePackage(packageName string) {
	data := struct {
		Name string
	}{
		packageName,
	}
	err := gen.templates.ExecuteTemplate(gen.w, "header.tmpl", data)
	d.Exp.NoError(err)

	for _, t := range gen.pkg.UsingDeclarations {
		gen.write(t)
	}

	names := make([]string, 0, len(gen.pkg.NamedTypes))
	for n, _ := range gen.pkg.NamedTypes {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		gen.write(gen.pkg.NamedTypes[n])
	}
}

func (gen *codeGen) write(t parse.TypeRef) {
	t = gen.resolve(t)
	if gen.written[gen.userName(t)] {
		return
	}
	k := t.Desc.Kind()
	switch k {
	case parse.BlobKind, parse.BoolKind, parse.Float32Kind, parse.Float64Kind, parse.Int16Kind, parse.Int32Kind, parse.Int64Kind, parse.Int8Kind, parse.StringKind, parse.UInt16Kind, parse.UInt32Kind, parse.UInt64Kind, parse.UInt8Kind, parse.ValueKind:
		return
	case parse.EnumKind:
		gen.writeEnum(t)
	case parse.ListKind:
		gen.writeList(t)
	case parse.MapKind:
		gen.writeMap(t)
	case parse.RefKind:
		gen.writeRef(t)
	case parse.SetKind:
		gen.writeSet(t)
	case parse.StructKind:
		gen.writeStruct(t)
	default:
		panic("unreachable")
	}
}

func (gen *codeGen) writeTemplate(tmpl string, t parse.TypeRef, data interface{}) {
	err := gen.templates.ExecuteTemplate(gen.w, tmpl, data)
	d.Exp.NoError(err)
	gen.written[gen.userName(t)] = true
}

func (gen *codeGen) writeStruct(t parse.TypeRef) {
	desc := t.Desc.(parse.StructDesc)
	data := struct {
		Name          string
		Fields        []parse.Field
		Choices       []parse.Field
		HasUnion      bool
		UnionZeroType parse.TypeRef
		CanUseDef     bool
	}{
		gen.userName(t),
		desc.Fields,
		nil,
		desc.Union != nil,
		parse.TypeRef{Desc: parse.PrimitiveDesc(parse.UInt32Kind)},
		gen.canUseDef(t),
	}
	if data.HasUnion {
		data.Choices = desc.Union.Choices
		data.UnionZeroType = data.Choices[0].T
	}
	gen.writeTemplate("struct.tmpl", t, data)
	for _, f := range desc.Fields {
		gen.write(f.T)
	}
	if data.HasUnion {
		for _, f := range desc.Union.Choices {
			gen.write(f.T)
		}
	}
}

func (gen *codeGen) writeList(t parse.TypeRef) {
	elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
	data := struct {
		Name      string
		ElemType  parse.TypeRef
		CanUseDef bool
	}{
		gen.userName(t),
		elemTypes[0],
		gen.canUseDef(t),
	}
	gen.writeTemplate("list.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeMap(t parse.TypeRef) {
	elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
	data := struct {
		Name      string
		KeyType   parse.TypeRef
		ValueType parse.TypeRef
		CanUseDef bool
	}{
		gen.userName(t),
		elemTypes[0],
		elemTypes[1],
		gen.canUseDef(t),
	}
	gen.writeTemplate("map.tmpl", t, data)
	gen.write(elemTypes[0])
	gen.write(elemTypes[1])
}

func (gen *codeGen) writeRef(t parse.TypeRef) {
	elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
	data := struct {
		Name     string
		ElemType parse.TypeRef
	}{
		gen.userName(t),
		elemTypes[0],
	}
	gen.writeTemplate("ref.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeSet(t parse.TypeRef) {
	elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
	data := struct {
		Name      string
		ElemType  parse.TypeRef
		CanUseDef bool
	}{
		gen.userName(t),
		elemTypes[0],
		gen.canUseDef(t),
	}
	gen.writeTemplate("set.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeEnum(t parse.TypeRef) {
	data := struct {
		Name string
		Ids  []string
	}{
		t.Name,
		t.Desc.(parse.EnumDesc).IDs,
	}
	gen.writeTemplate("enum.tmpl", t, data)
}

// We use a go map as the def for Set and Map. These cannot have a key that is a
// Set, Map or a List because slices and maps are not comparable in go.
func (gen *codeGen) canUseDef(t parse.TypeRef) bool {
	cache := map[string]bool{}

	var rec func(t parse.TypeRef, inKey bool) bool
	rec = func(t parse.TypeRef, inKey bool) bool {
		t = gen.resolve(t)
		switch t.Desc.Kind() {
		case parse.ListKind:
			return !inKey && rec(t.Desc.(parse.CompoundDesc).ElemTypes[0], inKey)
		case parse.SetKind:
			return !inKey && rec(t.Desc.(parse.CompoundDesc).ElemTypes[0], true)
		case parse.MapKind:
			elemTypes := t.Desc.(parse.CompoundDesc).ElemTypes
			return !inKey && rec(elemTypes[0], true) && rec(elemTypes[1], false)
		case parse.StructKind:
			userName := gen.userName(t)
			// Only structs can be recursive
			if b, ok := cache[userName]; ok {
				return b
			}

			if gen.isStructRecursive(t) {
				cache[userName] = false
				return false
			}

			cache[userName] = true
			for _, f := range t.Desc.(parse.StructDesc).Fields {
				if f.T.Equals(t) {
					cache[userName] = false
					return false
				}
				if !rec(f.T, inKey) {
					cache[userName] = false
					return false
				}
			}
			return true
		default:
			return true
		}
	}

	return rec(t, false)
}

func (gen *codeGen) isStructRecursive(t parse.TypeRef) bool {
	// A go struct cannot refer to itself unless there is an intermediate slice or map.
	top := true
	var rec func(t2 parse.TypeRef) bool
	rec = func(t2 parse.TypeRef) bool {
		if top {
			top = false
		} else if t2.Equals(t) {
			return true
		}
		t2 = gen.resolve(t2)
		switch t2.Desc.Kind() {
		case parse.StructKind:
			for _, f := range t2.Desc.(parse.StructDesc).Fields {
				if rec(f.T) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}

	return rec(t)
}
