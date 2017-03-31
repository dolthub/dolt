package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

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
//       struct{foo?:number,bar:string|blob,baz?:bool}
//
// Anytime any of the above cases generates a union as output, the same process
// is applied to that union recursively.
func (tc *TypeCache) makeSimplifiedType(intersectStructs bool, in ...*Type) *Type {
	ts := make(typeset, len(in))
	for _, t := range in {
		// De-cycle so that we handle cycles explicitly below. Otherwise, we would implicitly crawl
		// cycles and recurse forever.
		ts.Add(ToUnresolvedType(t))
	}

	// Impl de-cycles internally.
	return tc.makeSimplifiedTypeImpl(ts, intersectStructs)
}

// typeset is a helper that aggregates the unique set of input types for this algorithm, flattening
// any unions recursively.
type typeset map[hash.Hash]*Type

func (ts typeset) Add(t *Type) {
	switch t.Kind() {
	case UnionKind:
		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			ts.Add(et)
		}
	default:
		ts[t.Hash()] = t
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
func (tc *TypeCache) makeSimplifiedTypeImpl(in typeset, intersectStructs bool) *Type {
	type how struct {
		k NomsKind
		n string
	}

	out := make(typeSlice, 0, len(in))
	groups := map[how]typeset{}
	for _, t := range in {
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
			for _, t := range ts {
				out = append(out, t)
			}
			continue
		}

		var r *Type
		switch h.k {
		case RefKind:
			r = tc.simplifyContainers(h.k, ts, intersectStructs)
		case SetKind:
			r = tc.simplifyContainers(h.k, ts, intersectStructs)
		case ListKind:
			r = tc.simplifyContainers(h.k, ts, intersectStructs)
		case MapKind:
			r = tc.simplifyMaps(ts, intersectStructs)
		case StructKind:
			r = tc.simplifyStructs(h.n, ts, intersectStructs)
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

	sort.Sort(out)

	for i := 1; i < len(out); i++ {
		if !unionLess(out[i-1], out[i]) {
			panic("Invalid union order!!!")
		}
	}

	tc.Lock()
	defer tc.Unlock()

	return tc.getCompoundType(UnionKind, out...)
}

func (tc *TypeCache) simplifyContainers(expectedKind NomsKind, ts typeset, intersectStructs bool) *Type {
	elemTypes := make(typeset, len(ts))
	for _, t := range ts {
		d.Chk.True(expectedKind == t.Kind())
		elemTypes.Add(t.Desc.(CompoundDesc).ElemTypes[0])
	}

	elemType := tc.makeSimplifiedTypeImpl(elemTypes, intersectStructs)

	tc.Lock()
	defer tc.Unlock()

	return tc.getCompoundType(expectedKind, elemType)
}

func (tc *TypeCache) simplifyMaps(ts typeset, intersectStructs bool) *Type {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for _, t := range ts {
		d.Chk.True(MapKind == t.Kind())
		desc := t.Desc.(CompoundDesc)
		keyTypes.Add(desc.ElemTypes[0])
		valTypes.Add(desc.ElemTypes[1])
	}

	kt := tc.makeSimplifiedTypeImpl(keyTypes, intersectStructs)
	vt := tc.makeSimplifiedTypeImpl(valTypes, intersectStructs)

	tc.Lock()
	defer tc.Unlock()

	return tc.getCompoundType(MapKind, kt, vt)
}

func (tc *TypeCache) simplifyStructs(expectedName string, ts typeset, intersectStructs bool) *Type {
	allFields := make([]structFields, 0, len(ts))
	for _, t := range ts {
		desc := t.Desc.(StructDesc)
		d.PanicIfFalse(expectedName == desc.Name)
		allFields = append(allFields, desc.fields)
	}

	fields := simplifyStructFields(tc, allFields, intersectStructs)

	tc.Lock()
	defer tc.Unlock()

	return tc.makeStructType(expectedName, fields)
}

func replaceAndCollectStructTypes(tc *TypeCache, t *Type) (*Type, map[string]map[*Type]struct{}) {
	collected := map[string]map[*Type]struct{}{}
	out, changed := walkStructTypes(tc, t, nil, func(t *Type, cycle bool) (*Type, bool) {
		name := t.Desc.(StructDesc).Name

		if !cycle {
			// Do not collect if a cycle. We will collect when we get it on the wayt back.
			s := collected[name]
			if s == nil {
				s = map[*Type]struct{}{}
			}
			s[t] = struct{}{}
			collected[name] = s
		}
		tc.Lock()
		defer tc.Unlock()
		return tc.makeStructType(name, nil), true
	})
	d.PanicIfFalse(len(collected) > 0 == changed)
	return out, collected
}

func inlineStructTypes(tc *TypeCache, t *Type, defs map[string]*Type) *Type {
	out, _ := walkStructTypes(tc, t, nil, func(t *Type, cycle bool) (*Type, bool) {
		d.PanicIfTrue(cycle)
		st, ok := defs[t.Desc.(StructDesc).Name]
		d.PanicIfFalse(ok)
		return st, true
	})

	return out
}

func simplifyStructFields(tc *TypeCache, in []structFields, intersectStructs bool) structFields {
	// We gather all the fields/types into allFields. If the number of
	// times a field name is present is less that then number of types we
	// are simplifying then the field must be optional.
	// If we see an optional field we do not increment the count for it and
	// it will be treated as optional in the end.

	// If intersectStructs is true we need to pick the more restrictive version (n: T over n?: T).
	type fieldTypeInfo struct {
		anyNonOptional bool
		count          int
		typeset        typeset
	}
	allFields := map[string]fieldTypeInfo{}

	for _, ff := range in {
		for _, f := range ff {
			name := f.Name
			t := f.Type
			optional := f.Optional
			fti, ok := allFields[name]
			if !ok {
				fti = fieldTypeInfo{
					typeset: typeset{},
				}
			}
			fti.typeset.Add(t)
			if !optional {
				fti.count++
				fti.anyNonOptional = true
			}
			allFields[name] = fti
		}
	}

	count := len(in)
	fields := make(structFields, 0, count)
	for name, fti := range allFields {
		fields = append(fields, StructField{
			Name:     name,
			Type:     tc.makeSimplifiedTypeImpl(fti.typeset, intersectStructs),
			Optional: !(intersectStructs && fti.anyNonOptional) && fti.count < count,
		})
	}

	sort.Sort(fields)

	return fields
}
