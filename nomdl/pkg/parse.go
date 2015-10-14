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
	resolveNamespaces(&i, imports, GetDeps(depRefs, cs))
	return Parsed{
		types.PackageDef{Dependencies: depRefs, Types: i.Types},
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
	types.PackageDef
	Name              string
	UsingDeclarations []types.TypeRef
}

type intermediate struct {
	Name              string
	Aliases           map[string]string
	UsingDeclarations []types.TypeRef
	Types             []types.TypeRef
}

func findTypeByName(n string, ts []types.TypeRef) (types.TypeRef, bool) {
	for _, t := range ts {
		if n == t.Name() {
			return t, true
		}
	}
	return types.TypeRef{}, false
}

func (p Parsed) GetNamedType(name string) types.TypeRef {
	t, _ := findTypeByName(name, p.Types)
	return t
}

func (i *intermediate) GetNamedType(name string) types.TypeRef {
	t, _ := findTypeByName(name, i.Types)
	return t
}

func (i *intermediate) checkLocal(t types.TypeRef) bool {
	if t.Namespace() == "" {
		d.Chk.Equal(ref.Ref{}, t.PackageRef())
		_, ok := findTypeByName(t.Name(), i.Types)
		d.Exp.True(ok, "Could not find type %s in current package.", t.Name())
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
			imports[alias] = types.WriteValue(parsedDep.New().NomsValue(), cs)
		} else {
			imports[alias] = r
		}
	}
	return imports
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
	target, ok := aliases[t.Namespace()]
	d.Exp.True(ok, "Could not find import aliased to %s", t.Namespace())
	d.Exp.True(deps[target].HasNamedType(t.Name()), "Could not find type %s in package %s (aliased to %s).", t.Name(), target.String(), t.Namespace())
	return types.MakeTypeRef(t.Name(), target)
}
