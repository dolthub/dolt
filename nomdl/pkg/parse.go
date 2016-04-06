package pkg

import (
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Parsed represents a parsed Noms type package, which has some additional metadata beyond that which is present in a types.Package.
// UsingDeclarations is kind of a hack to indicate specializations of Noms containers that need to be generated. These should all be one of ListKind, SetKind, MapKind or RefKind, and Desc should be a CompoundDesc instance.
type Parsed struct {
	types.Package
	Name              string
	UsingDeclarations []types.Type
	AliasNames        map[ref.Ref]string
}

// ParseNomDL reads a Noms package specification from r and returns a Package. Errors will be annotated with packageName and thrown.
func ParseNomDL(packageName string, r io.Reader, includePath string, vrw types.ValueReadWriter) Parsed {
	i := runParser(packageName, r)
	i.Name = packageName
	imports := resolveImports(i.Aliases, includePath, vrw)
	deps := importsToDeps(imports)

	resolveLocalOrdinals(&i)
	resolveNamespaces(&i, imports, getDeps(deps, vrw))

	// Transpose imports
	aliasNames := map[ref.Ref]string{}
	for k, v := range imports {
		aliasNames[v] = k
	}

	return Parsed{
		types.NewPackage(i.Types, deps),
		i.Name,
		i.UsingDeclarations,
		aliasNames,
	}
}

type intermediate struct {
	Name              string
	Aliases           map[string]string
	UsingDeclarations []types.Type
	Types             []types.Type
}

func runParser(logname string, r io.Reader) intermediate {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(intermediate)
}

func resolveLocalOrdinals(p *intermediate) {
	var rec func(t types.Type) types.Type
	resolveFields := func(fields []types.Field) {
		for idx, f := range fields {
			f.T = rec(f.T)
			fields[idx] = f
		}
	}
	rec = func(t types.Type) types.Type {
		if t.IsUnresolved() {
			if t.Namespace() == "" && !t.HasOrdinal() {
				ordinal := indexOf(t, p.Types)
				d.Chk.True(ordinal >= 0 && int(ordinal) < len(p.Types), "Invalid reference: %s", t.Name())
				return types.MakeType(ref.Ref{}, int16(ordinal))
			}

			return t
		}

		switch t.Kind() {
		case types.ListKind, types.SetKind, types.RefKind:
			return types.MakeCompoundType(t.Kind(), rec(t.Desc.(types.CompoundDesc).ElemTypes[0]))
		case types.MapKind:
			elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
			return types.MakeCompoundType(t.Kind(), rec(elemTypes[0]), rec(elemTypes[1]))
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

func indexOf(t types.Type, ts []types.Type) int16 {
	for i, tt := range ts {
		if tt.Name() == t.Name() && tt.Namespace() == "" {
			return int16(i)
		}
	}
	return -1
}

func resolveNamespaces(p *intermediate, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) {
	var rec func(t types.Type) types.Type
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
	rec = func(t types.Type) types.Type {
		if t.IsUnresolved() {
			if p.checkLocal(t) {
				return t
			}
			t = resolveNamespace(t, aliases, deps)
		}
		switch t.Kind() {
		case types.UnresolvedKind:
			d.Chk.True(t.HasPackageRef(), "should resolve again")
		case types.ListKind, types.SetKind, types.RefKind:
			return types.MakeCompoundType(t.Kind(), rec(t.Desc.(types.CompoundDesc).ElemTypes[0]))
		case types.MapKind:
			elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
			return types.MakeCompoundType(t.Kind(), rec(elemTypes[0]), rec(elemTypes[1]))
		case types.StructKind:
			resolveFields(t.Desc.(types.StructDesc).Fields)
			resolveFields(t.Desc.(types.StructDesc).Union)
		}

		if t.IsUnresolved() {
			return rec(t)
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

func (i *intermediate) checkLocal(t types.Type) bool {
	if t.Namespace() == "" {
		d.Chk.True(t.HasOrdinal(), "Invalid local reference")
		return true
	}
	return false
}

func resolveNamespace(t types.Type, aliases map[string]ref.Ref, deps map[ref.Ref]types.Package) types.Type {
	pkgRef, ok := aliases[t.Namespace()]
	d.Exp.True(ok, "Could not find import aliased to %s", t.Namespace())
	d.Chk.NotEqual("", t.Name())
	ordinal := deps[pkgRef].GetOrdinal(t.Name())
	d.Exp.NotEqual(int64(-1), ordinal, "Could not find type %s in package %s (aliased to %s).", t.Name(), pkgRef.String(), t.Namespace())
	d.Chk.False(pkgRef.IsEmpty())
	return types.MakeType(pkgRef, int16(ordinal))
}

// expandStruct takes a struct definition and expands the internal structs created for unions.
func expandStruct(t types.Type, ordinal int) []types.Type {
	d.Chk.Equal(types.StructKind, t.Kind())
	ts := []types.Type{t}
	ordinal++

	doFields := func(fields []types.Field) []types.Field {
		rv := make([]types.Field, len(fields))
		for i, f := range fields {
			if f.T.Kind() == types.StructKind {
				newType := expandStruct(f.T, ordinal)
				ts = append(ts, newType...)
				rv[i] = types.Field{f.Name, types.MakeType(ref.Ref{}, int16(ordinal)), f.Optional}
				ordinal += len(newType)
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
		ts[0] = types.MakeStructType(t.Name(), fields, choices)
	}
	return ts
}
