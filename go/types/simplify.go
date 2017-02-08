package types

import (
	"github.com/attic-labs/noms/go/d"
)

// makeSimplifiedUnion returns a type that is a supertype of all the input types, but is much
// smaller and less complex than a straight union of all those types would be.
//
// The resulting type is guaranteed to:
// a. be a supertype of all input types
// b. have no direct children that are unions
// c. have at most one element each of kind Ref, Set, List, and Map
// d. have at most one struct element with a given name
// e. all union types reachable from it also fulfill b-e
//
// The simplification is created roughly as follows:
//
// - The input types are deduplicated
// - Any unions in the input set are "flattened" into the input set
// - The inputs are grouped into categories:
//    - ref
//    - list
//    - set
//    - map
//    - struct, by name (each unique struct name will have its own group)
// - The ref, set, and list groups are collapsed like so:
//     {Ref<A>,Ref<B>,...} -> Ref<A|B|...>
// - The map group is collapsed like so:
//     {Map<K1,V1>|Map<K2,V2>...} -> Map<K1|K2,V1|V2>
// - Each struct group is collapsed like so:
//     {struct{foo:number,bar:string}, struct{bar:bool, baz:blob}} ->
//       struct{bar:string|blob}
//
// Anytime any of the above cases generates a union as output, the same process
// is applied to that union recursively.
func makeSimplifiedUnion(in ...*Type) *Type {
	ts := make(typeset, len(in))
	for _, t := range in {
		// De-cycle so that we handle cycles explicitly below. Otherwise, we would implicitly crawl
		// cycles and recurse forever.
		t := ToUnresolvedType(t)
		ts[t] = struct{}{}
	}

	// makeSimplifiedUnionImpl de-cycles internally.
	return makeSimplifiedUnionImpl(ts)
}

// typeset is a helper that aggregates the unique set of input types for this algorithm, flattening
// any unions recursively.
type typeset map[*Type]struct{}

func (ts typeset) Add(t *Type) {
	switch t.Kind() {
	case UnionKind:
		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			ts.Add(et)
		}
	default:
		ts[t] = struct{}{}
	}
}

func newTypeset(t ...*Type) typeset {
	ts := make(typeset, len(t))
	for _, t := range t {
		ts.Add(t)
	}
	return ts
}

// makeSimplifiedUnionImpl is an implementation detail of makeSimplifiedUnion.
// Warning: Do not call this directly. It assumes its input has been de-cycled using
// ToUnresolvedType() and will infinitely recurse on cyclic types otherwise.
func makeSimplifiedUnionImpl(in typeset) *Type {
	type how struct {
		k NomsKind
		n string
	}

	out := make([]*Type, 0, len(in))
	groups := map[how]typeset{}
	for t := range in {
		var h how
		switch t.Kind() {
		case RefKind, SetKind, ListKind, MapKind:
			h = how{k: t.Kind()}
		case StructKind:
			h = how{k: t.Kind(), n: t.Desc.(StructDesc).Name}
		default:
			out = append(out, t)
			continue
		}
		g := groups[h]
		if g == nil {
			g = typeset{}
			groups[h] = g
		}
		g.Add(t)
	}

	for h, ts := range groups {
		if len(ts) == 1 {
			for t := range ts {
				out = append(out, t)
			}
			continue
		}

		var r *Type
		switch h.k {
		case RefKind:
			r = simplifyRefs(ts)
		case SetKind:
			r = simplifySets(ts)
		case ListKind:
			r = simplifyLists(ts)
		case MapKind:
			r = simplifyMaps(ts)
		case StructKind:
			r = simplifyStructs(h.n, ts)
		}
		out = append(out, r)
	}

	for i, t := range out {
		t = ToUnresolvedType(t)
		out[i] = resolveStructCycles(t, nil)
	}

	if len(out) == 1 {
		return out[0]
	}

	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.makeUnionType(out...)
}

func simplifyRefs(ts typeset) *Type {
	return simplifyContainers(RefKind, MakeRefType, ts)
}

func simplifySets(ts typeset) *Type {
	return simplifyContainers(SetKind, MakeSetType, ts)
}

func simplifyLists(ts typeset) *Type {
	return simplifyContainers(ListKind, MakeListType, ts)
}

func simplifyContainers(expectedKind NomsKind, makeContainer func(elem *Type) *Type, ts typeset) *Type {
	elemTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(expectedKind == t.Kind())
		elemTypes.Add(t.Desc.(CompoundDesc).ElemTypes[0])
	}
	return makeContainer(makeSimplifiedUnionImpl(elemTypes))
}

func simplifyMaps(ts typeset) *Type {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(MapKind == t.Kind())
		desc := t.Desc.(CompoundDesc)
		keyTypes.Add(desc.ElemTypes[0])
		valTypes.Add(desc.ElemTypes[1])
	}
	return MakeMapType(
		makeSimplifiedUnionImpl(keyTypes),
		makeSimplifiedUnionImpl(valTypes))
}

func simplifyStructs(expectedName string, ts typeset) *Type {
	commonFields := map[string]typeset{}

	first := true
	for t := range ts {
		d.Chk.True(StructKind == t.Kind())
		desc := t.Desc.(StructDesc)
		d.Chk.True(expectedName == desc.Name)
		if first {
			for _, f := range desc.fields {
				ts := typeset{}
				ts.Add(f.t)
				commonFields[f.name] = ts
			}
			first = false
		} else {
			for n, ts := range commonFields {
				t := desc.Field(n)
				if t != nil {
					ts.Add(t)
				} else {
					delete(commonFields, n)
				}
			}
		}
	}

	fm := make(FieldMap, len(ts))
	for n, ts := range commonFields {
		fm[n] = makeSimplifiedUnionImpl(ts)
	}

	return MakeStructTypeFromFields(expectedName, fm)
}
