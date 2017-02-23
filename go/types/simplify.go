package types

import "github.com/attic-labs/noms/go/d"

// makeSimplifiedType returns a type that is a supertype of all the input types but is much
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
//     {struct{foo:number,bar:string}, struct{bar:blob, baz:bool}} ->
//       struct{bar:string|blob}
//
// Anytime any of the above cases generates a union as output, the same process
// is applied to that union recursively.
func makeSimplifiedType(in ...*Type) *Type {
	ts := make(typeset, len(in))
	for _, t := range in {
		// De-cycle so that we handle cycles explicitly below. Otherwise, we would implicitly crawl
		// cycles and recurse forever.
		t := ToUnresolvedType(t)
		ts[t] = struct{}{}
	}

	// Impl de-cycles internally.
	return makeSimplifiedTypeImpl(ts, false)
}

// makeSimplifiedType2 returns a type that results from merging all input types.
//
// The result is similar makeSimplifiedType with one exception:
//
// Each matching Struct found in the input will be merged into a single struct containing all fields
// found in the input structs.
//
// Each struct is expanded like so:
//     {struct{foo:number,bar:string}, struct{bar:blob, baz:bool}} ->
//       struct{foo:number, bar:string|blob, baz:bool}
//
func makeSimplifedType2(in ...*Type) *Type {
	ts := make(typeset, len(in))
	for _, t := range in {
		// De-cycle so that we handle cycles explicitly below. Otherwise, we would implicitly crawl
		// cycles and recurse forever.
		t := ToUnresolvedType(t)
		ts[t] = struct{}{}
	}

	// Impl de-cycles internally.
	return makeSimplifiedTypeImpl(ts, true)
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

// makeSimplifiedTypeImpl is an implementation detail.
// Warning: Do not call this directly. It assumes its input has been de-cycled using
// ToUnresolvedType() and will infinitely recurse on cyclic types otherwise.
func makeSimplifiedTypeImpl(in typeset, merge bool) *Type {
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
			r = simplifyRefs(ts, merge)
		case SetKind:
			r = simplifySets(ts, merge)
		case ListKind:
			r = simplifyLists(ts, merge)
		case MapKind:
			r = simplifyMaps(ts, merge)
		case StructKind:
			r = simplifyStructs(h.n, ts, merge)
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

func simplifyRefs(ts typeset, merge bool) *Type {
	return simplifyContainers(RefKind, MakeRefType, ts, merge)
}

func simplifySets(ts typeset, merge bool) *Type {
	return simplifyContainers(SetKind, MakeSetType, ts, merge)
}

func simplifyLists(ts typeset, merge bool) *Type {
	return simplifyContainers(ListKind, MakeListType, ts, merge)
}

func simplifyContainers(expectedKind NomsKind, makeContainer func(elem *Type) *Type, ts typeset, merge bool) *Type {
	elemTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(expectedKind == t.Kind())
		elemTypes.Add(t.Desc.(CompoundDesc).ElemTypes[0])
	}
	return makeContainer(makeSimplifiedTypeImpl(elemTypes, merge))
}

func simplifyMaps(ts typeset, merge bool) *Type {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(MapKind == t.Kind())
		desc := t.Desc.(CompoundDesc)
		keyTypes.Add(desc.ElemTypes[0])
		valTypes.Add(desc.ElemTypes[1])
	}
	return MakeMapType(
		makeSimplifiedTypeImpl(keyTypes, merge),
		makeSimplifiedTypeImpl(valTypes, merge))
}

func simplifyStructs(expectedName string, ts typeset, merge bool) *Type {
	if merge {
		return mergeStructs(expectedName, ts)
	}
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
		fm[n] = makeSimplifiedTypeImpl(ts, false)
	}

	return MakeStructTypeFromFields(expectedName, fm)
}

func mergeStructs(expectedName string, ts typeset) *Type {
	unionFields := map[string]typeset{}
	for t := range ts {
		d.Chk.True(StructKind == t.Kind())
		desc := t.Desc.(StructDesc)
		d.Chk.True(expectedName == desc.Name)
		for _, f := range desc.fields {
			ts, ok := unionFields[f.name]
			if !ok {
				ts = typeset{}
			}
			ts.Add(f.t)
			unionFields[f.name] = ts
		}
	}

	fm := make(FieldMap, len(ts))
	for n, ts := range unionFields {
		fm[n] = makeSimplifiedTypeImpl(ts, true)
	}

	return MakeStructTypeFromFields(expectedName, fm)
}
