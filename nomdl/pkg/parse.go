package pkg

import (
	"io"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// ParseNomDL reads a Noms package specification from r and returns a Package. Errors will be annotated with packageName and thrown.
func ParseNomDL(packageName string, r io.Reader, includePath string, cs chunks.ChunkStore) Parsed {
	i := runParser(packageName, r)
	i.Name = packageName
	imports := resolveImports(i.Aliases, includePath, cs)
	depRefs := types.SetOfRefOfPackageDef{}
	for _, target := range imports {
		depRefs[target] = true
	}

	resolveLocalOrdinals(&i)
	resolveNamespaces(&i, imports, GetDeps(depRefs, cs))
	return Parsed{
		types.PackageDef{Dependencies: depRefs, Types: i.Types}.New(),
		i.Name,
		i.UsingDeclarations,
	}
}

// GetDeps reads the types.Package objects referred to by depRefs out of cs and returns a map of ref: PackageDef.
func GetDeps(depRefs types.SetOfRefOfPackageDef, cs chunks.ChunkStore) map[ref.Ref]types.Package {
	deps := map[ref.Ref]types.Package{}
	for depRef := range depRefs {
		v := types.ReadValue(depRef, cs)
		d.Chk.NotNil(v, "Importing package by ref %s failed.", depRef.String())
		deps[depRef] = types.PackageFromVal(v)
	}
	return deps
}

// Parsed represents a parsed Noms type package, which has some additional metadata beyond that which is present in a types.Package.
// UsingDeclarations is kind of a hack to indicate specializations of Noms containers that need to be generated. These should all be one of ListKind, SetKind, MapKind or RefKind, and Desc should be a CompoundDesc instance.
type Parsed struct {
	types.Package
	Name              string
	UsingDeclarations []types.TypeRef
}

type intermediate struct {
	Name              string
	Aliases           map[string]string
	UsingDeclarations []types.TypeRef
	Types             []types.TypeRef
}

func indexOf(t types.TypeRef, ts []types.TypeRef) int16 {
	for i, tt := range ts {
		if tt.Name() == t.Name() && tt.Namespace() == "" {
			return int16(i)
		}
	}
	return -1
}

func (i *intermediate) checkLocal(t types.TypeRef) bool {
	if t.Namespace() == "" {
		d.Chk.True(t.HasOrdinal(), "Invalid local reference")
		return true
	}
	return false
}

func runParser(logname string, r io.Reader) intermediate {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(intermediate)
}

func resolveImports(aliases map[string]string, includePath string, cs chunks.ChunkStore) map[string]ref.Ref {
	canonicalize := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(includePath, path)
	}
	imports := map[string]ref.Ref{}

	for alias, target := range aliases {
		var r ref.Ref
		if d.Try(func() { r = ref.Parse(target) }) != nil {
			canonical := canonicalize(target)
			inFile, err := os.Open(canonical)
			d.Chk.NoError(err)
			defer inFile.Close()
			parsedDep := ParseNomDL(alias, inFile, filepath.Dir(canonical), cs)
			imports[alias] = types.WriteValue(parsedDep.NomsValue(), cs)
		} else {
			imports[alias] = r
		}
	}
	return imports
}

func resolveLocalOrdinals(p *intermediate) {
	var rec func(t types.TypeRef) types.TypeRef
	resolveFields := func(fields []types.Field) {
		for idx, f := range fields {
			f.T = rec(f.T)
			fields[idx] = f
		}
	}
	rec = func(t types.TypeRef) types.TypeRef {
		if t.IsUnresolved() {
			if t.Namespace() == "" && !t.HasOrdinal() {
				ordinal := indexOf(t, p.Types)
				d.Chk.True(ordinal >= 0 && int(ordinal) < len(p.Types), "Invalid reference: %s", t.Name())
				return types.MakeTypeRef(t.PackageRef(), int16(ordinal))
			}
			return t
		}

		switch t.Kind() {
		case types.ListKind, types.SetKind, types.RefKind:
			return types.MakeCompoundTypeRef(t.Name(), t.Kind(), rec(t.Desc.(types.CompoundDesc).ElemTypes[0]))
		case types.MapKind:
			elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
			return types.MakeCompoundTypeRef(t.Name(), t.Kind(), rec(elemTypes[0]), rec(elemTypes[1]))
		case types.StructKind:
			resolveFields(t.Desc.(types.StructDesc).Fields)
			resolveFields(t.Desc.(types.StructDesc).Union)
		}
		return t
	}

	for i, t := range p.Types {
		p.Types[i] = rec(t)
	}
	for i, t := range p.UsingDeclarations {
		p.UsingDeclarations[i] = rec(t)
	}
}

func resolveNamespaces(p *intermediate, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) {
	var rec func(t types.TypeRef) types.TypeRef
	resolveFields := func(fields []types.Field) {
		for idx, f := range fields {
			if f.T.IsUnresolved() {
				if p.checkLocal(f.T) {
					continue
				}
				f.T = resolveNamespace(f.T, aliases, deps)
			} else {
				f.T = rec(f.T)
			}

			d.Chk.True(!f.T.IsUnresolved() || f.T.HasOrdinal())
			fields[idx] = f
		}
	}
	rec = func(t types.TypeRef) types.TypeRef {
		if t.IsUnresolved() {
			if p.checkLocal(t) {
				return t
			}
			t = resolveNamespace(t, aliases, deps)
		}
		switch t.Kind() {
		case types.ListKind, types.SetKind, types.RefKind:
			return types.MakeCompoundTypeRef(t.Name(), t.Kind(), rec(t.Desc.(types.CompoundDesc).ElemTypes[0]))
		case types.MapKind:
			elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
			return types.MakeCompoundTypeRef(t.Name(), t.Kind(), rec(elemTypes[0]), rec(elemTypes[1]))
		case types.StructKind:
			resolveFields(t.Desc.(types.StructDesc).Fields)
			resolveFields(t.Desc.(types.StructDesc).Union)
		}

		d.Chk.True(!t.IsUnresolved() || t.HasOrdinal())
		return t
	}

	for i, t := range p.Types {
		p.Types[i] = rec(t)
	}
	for i, t := range p.UsingDeclarations {
		p.UsingDeclarations[i] = rec(t)
	}
}

func resolveNamespace(t types.TypeRef, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) types.TypeRef {
	pkgRef, ok := aliases[t.Namespace()]
	d.Exp.True(ok, "Could not find import aliased to %s", t.Namespace())
	d.Chk.NotEqual("", t.Name())
	ordinal := deps[pkgRef].GetOrdinal(t.Name())
	d.Exp.NotEqual(int64(-1), ordinal, "Could not find type %s in package %s (aliased to %s).", t.Name(), pkgRef.String(), t.Namespace())
	return types.MakeTypeRef(pkgRef, int16(ordinal))
}

// expandStruct takes a struct definition and expands the internal structs created for unions.
func expandStruct(t types.TypeRef, ordinal int) []types.TypeRef {
	d.Chk.Equal(types.StructKind, t.Kind())
	ts := []types.TypeRef{t}
	ordinal++

	doFields := func(fields []types.Field) []types.Field {
		rv := make([]types.Field, len(fields))
		for i, f := range fields {
			if f.T.Kind() == types.StructKind {
				newTypeRefs := expandStruct(f.T, ordinal)
				ts = append(ts, newTypeRefs...)
				rv[i] = types.Field{f.Name, types.MakeTypeRef(ref.Ref{}, int16(ordinal)), f.Optional}
				ordinal += len(newTypeRefs)
			} else {
				rv[i] = f
			}
		}
		return rv
	}

	desc := t.Desc.(types.StructDesc)
	fields := doFields(desc.Fields)

	var choices types.Choices
	if desc.Union != nil {
		choices = doFields(desc.Union)
	}

	if len(ts) != 1 {
		ts[0] = types.MakeStructTypeRef(t.Name(), fields, choices)
	}
	return ts
}
