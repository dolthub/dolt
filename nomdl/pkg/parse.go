package pkg

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// ParseNomDL reads a Noms package specification from r and returns a Package. Errors will be annotated with packageName and thrown.
func ParseNomDL(packageName string, r io.Reader, cs chunks.ChunkSource) Parsed {
	i := runParser(packageName, r)
	i.Name = packageName
	aliases := resolveImports(i)
	depRefs := types.SetOfRefOfPackageDef{}
	for _, target := range aliases {
		depRefs[target] = true
	}
	resolveNamespaces(i.NamedTypes, aliases, GetDeps(depRefs, cs))
	return Parsed{
		types.PackageDef{Dependencies: depRefs, NamedTypes: i.NamedTypes},
		i.Name,
		i.UsingDeclarations,
	}
}

// GetDeps reads the types.Package objects referred to by depRefs out of cs and returns a map of ref: PackageDef.
func GetDeps(depRefs types.SetOfRefOfPackageDef, cs chunks.ChunkSource) map[ref.Ref]types.Package {
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
	NamedTypes        map[string]types.TypeRef
}

func runParser(logname string, r io.Reader) intermediate {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(intermediate)
}

func resolveImports(pkg intermediate) map[string]ref.Ref {
	aliases := map[string]ref.Ref{}

	for alias, target := range pkg.Aliases {
		var r ref.Ref
		if d.Try(func() { r = ref.Parse(target) }) != nil {
			continue // will support import by path later
		}
		aliases[alias] = r
	}
	return aliases
}

func resolveNamespaces(namedTypes map[string]types.TypeRef, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) {
	var rec func(t types.TypeRef) types.TypeRef
	resolveFields := func(fields []types.Field) {
		for idx, f := range fields {
			if f.T.IsUnresolved() {
				if f.T.Namespace() == "" {
					d.Chk.Equal(ref.Ref{}, f.T.PackageRef())
					_, ok := namedTypes[f.T.Name()]
					d.Exp.True(ok, "Could not find type %s in current package.", f.T.Name())
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
		if t.Kind() == types.StructKind {
			resolveFields(t.Desc.(types.StructDesc).Fields)
			resolveFields(t.Desc.(types.StructDesc).Union)
		}
		return t
	}

	for n, t := range namedTypes {
		if t.IsUnresolved() {
			namedTypes[n] = resolveNamespace(t, aliases, deps)
			continue
		}
		namedTypes[n] = rec(t)
	}

}

func resolveNamespace(t types.TypeRef, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) types.TypeRef {
	target, ok := aliases[t.Namespace()]
	d.Exp.True(ok, "Could not find import aliased to %s", t.Namespace())
	d.Exp.True(deps[target].NamedTypes().Has(t.Name()), "Could not find type %s in package %s (aliased to %s).", t.Name(), target.String(), t.Namespace())
	return types.MakeTypeRef(t.Name(), target)
}
