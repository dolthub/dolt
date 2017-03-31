package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
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
func makeSimplifiedType(intersectStructs bool, in ...*Type) *Type {
	seen := map[*Type]*Type{}
	pending := map[string]*unsimplifiedStruct{}

	ts := make(typeset, len(in))
	for _, t := range in {
		out, _ := removeAndCollectStructFields(t, seen, pending)
		ts.Add(out)
	}

	result := makeSimplifiedTypeImpl(ts, intersectStructs)
	for _, rec := range pending {
		desc := rec.t.Desc.(StructDesc)
		desc.fields = simplifyStructFields(rec.fieldSets, intersectStructs)
		rec.t.Desc = desc
	}
	return result
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
func makeSimplifiedTypeImpl(in typeset, intersectStructs bool) *Type {
	type how struct {
		k NomsKind
		n string
	}

	out := make(typeSlice, 0, len(in))
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
			r = simplifyContainers(h.k, ts, intersectStructs)
		case SetKind:
			r = simplifyContainers(h.k, ts, intersectStructs)
		case ListKind:
			r = simplifyContainers(h.k, ts, intersectStructs)
		case MapKind:
			r = simplifyMaps(ts, intersectStructs)
		case StructKind:
			panic("unreachable") // we have alreade folded structs
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

	return makeCompoundType(UnionKind, out...)
}

func simplifyContainers(expectedKind NomsKind, ts typeset, intersectStructs bool) *Type {
	elemTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(expectedKind == t.Kind())
		elemTypes.Add(t.Desc.(CompoundDesc).ElemTypes[0])
	}

	elemType := makeSimplifiedTypeImpl(elemTypes, intersectStructs)

	return makeCompoundType(expectedKind, elemType)
}

func simplifyMaps(ts typeset, intersectStructs bool) *Type {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for t := range ts {
		d.Chk.True(MapKind == t.Kind())
		desc := t.Desc.(CompoundDesc)
		keyTypes.Add(desc.ElemTypes[0])
		valTypes.Add(desc.ElemTypes[1])
	}

	kt := makeSimplifiedTypeImpl(keyTypes, intersectStructs)
	vt := makeSimplifiedTypeImpl(valTypes, intersectStructs)

	return makeCompoundType(MapKind, kt, vt)
}

type unsimplifiedStruct struct {
	t         *Type
	fieldSets []structFields
}

func removeAndCollectStructFields(t *Type, seen map[*Type]*Type, pendingStructs map[string]*unsimplifiedStruct) (*Type, bool) {
	switch t.Kind() {
	case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind:
		return t, false
	case ListKind, MapKind, RefKind, SetKind, UnionKind:
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		changed := false
		newElemTypes := make(typeSlice, len(elemTypes))
		for i, et := range elemTypes {
			et2, c := removeAndCollectStructFields(et, seen, pendingStructs)
			newElemTypes[i] = et2
			changed = changed || c
		}
		if !changed {
			return t, false
		}

		return makeCompoundType(t.Kind(), newElemTypes...), true

	case StructKind:
		newStruct, found := seen[t]
		if found {
			return newStruct, true
		}

		desc := t.Desc.(StructDesc)
		name := desc.Name
		pending, ok := pendingStructs[name]
		if ok {
			newStruct = pending.t
		} else {
			newStruct = newType(StructDesc{Name: name})
			pending = &unsimplifiedStruct{newStruct, []structFields{}}
			pendingStructs[name] = pending
		}
		seen[t] = newStruct

		newFields := make(structFields, len(desc.fields))
		changed := false
		for i, f := range desc.fields {
			nt, c := removeAndCollectStructFields(f.Type, seen, pendingStructs)
			newFields[i] = StructField{Name: f.Name, Type: nt, Optional: f.Optional}
			changed = changed || c
		}

		if !changed {
			newFields = desc.fields
		}
		pending.fieldSets = append(pending.fieldSets, newFields)
		return newStruct, true

	case CycleKind:
		return t, false
	}

	panic("unreachable") // no more noms kinds
}

func simplifyStructFields(in []structFields, intersectStructs bool) structFields {
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
			Type:     makeSimplifiedTypeImpl(fti.typeset, intersectStructs),
			Optional: !(intersectStructs && fti.anyNonOptional) && fti.count < count,
		})
	}

	sort.Sort(fields)

	return fields
}
