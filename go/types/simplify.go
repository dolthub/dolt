package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

// simplifyType returns a type that is a super type of the input type but is
// much smaller and less complex than a straight union of all those types would
// be.
//
// The resulting type is guaranteed to:
// a. be a super type of the input type
// b. have all unions flattened (no union inside a union)
// c. have all unions folded, which means the union
//    1. have at most one element each of kind Ref, Set, List, and Map
//    2. have at most one struct element with a given name
// e. all named unions are pointing at the same simplified struct, which means
//    that all named unions with the same name form cycles.
// f. all cycle type that can be resolved have been resolved.
// g. all types reachable from it also fulfill b-f
//
// The union folding is created roughly as follows:
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
// All the above rules are applied recursively.
func simplifyType(t *Type, intersectStructs bool) *Type {
	if t.Desc.isSimplifiedForSure() {
		return t
	}

	// 1. Clone tree because we are going to mutate it
	//    1.1 Replace all named structs and cycle types with a single `struct Name {}`
	// 2. When a union type is found change its elemTypes as needed
	//    2.1 Merge unnamed structs
	// 3. Update the fields of all named structs

	namedStructs := map[string]structInfo{}

	clone := cloneTypeTreeAndReplaceNamedStructs(t, namedStructs)
	folded := foldUnions(clone, typeset{}, intersectStructs)

	for name, info := range namedStructs {
		if len(info.sources) == 0 {
			d.PanicIfTrue(name == "")
			info.instance.Desc = CycleDesc(name)
		} else {
			fields := foldStructTypesFieldsOnly(name, info.sources, typeset{}, intersectStructs)
			info.instance.Desc = StructDesc{name, fields}
		}
	}

	return folded
}

// typeset is a helper that aggregates the unique set of input types for this algorithm, flattening
// any unions recursively.
type typeset map[*Type]struct{}

func (ts typeset) add(t *Type) {
	switch t.TargetKind() {
	case UnionKind:
		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			ts.add(et)
		}
	default:
		ts[t] = struct{}{}
	}
}

func (ts typeset) has(t *Type) bool {
	_, ok := ts[t]
	return ok
}

type structInfo struct {
	instance *Type
	sources  typeset
}

func cloneTypeTreeAndReplaceNamedStructs(t *Type, namedStructs map[string]structInfo) *Type {
	getNamedStruct := func(name string, t *Type) *Type {
		record := namedStructs[name]
		if t.TargetKind() == StructKind {
			record.sources.add(t)
		}
		return record.instance
	}

	ensureInstance := func(name string) {
		if _, ok := namedStructs[name]; !ok {
			instance := newType(StructDesc{Name: name})
			namedStructs[name] = structInfo{instance, typeset{}}
		}
	}

	seenStructs := typeset{}
	var rec func(t *Type) *Type
	rec = func(t *Type) *Type {
		kind := t.TargetKind()
		switch kind {
		case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind:
			return t
		case ListKind, MapKind, RefKind, SetKind, UnionKind:
			elemTypes := make(typeSlice, len(t.Desc.(CompoundDesc).ElemTypes))
			for i, et := range t.Desc.(CompoundDesc).ElemTypes {
				elemTypes[i] = rec(et)
			}
			return newType(CompoundDesc{kind, elemTypes})
		case StructKind:
			desc := t.Desc.(StructDesc)
			name := desc.Name

			if name != "" {
				ensureInstance(name)
				if seenStructs.has(t) {
					return namedStructs[name].instance
				}
			} else if seenStructs.has(t) {
				// It is OK to use the same unnamed struct type in multiple places.
				// Do not clone it again.
				return t
			}
			seenStructs.add(t)

			fields := make(structTypeFields, len(desc.fields))
			for i, f := range desc.fields {
				fields[i] = StructField{f.Name, rec(f.Type), f.Optional}
			}
			newStruct := newType(StructDesc{name, fields})
			if name == "" {
				return newStruct
			}

			return getNamedStruct(name, newStruct)

		case CycleKind:
			name := string(t.Desc.(CycleDesc))
			d.PanicIfTrue(name == "")
			ensureInstance(name)
			return getNamedStruct(name, t)

		default:
			panic("Unknown noms kind")
		}
	}

	return rec(t)
}

func foldUnions(t *Type, seenStructs typeset, intersectStructs bool) *Type {
	kind := t.TargetKind()
	switch kind {
	case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind, CycleKind:
		break

	case ListKind, MapKind, RefKind, SetKind:
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		for i, et := range elemTypes {
			elemTypes[i] = foldUnions(et, seenStructs, intersectStructs)
		}

	case StructKind:
		if seenStructs.has(t) {
			return t
		}
		seenStructs.add(t)
		fields := t.Desc.(StructDesc).fields
		for i, f := range fields {
			fields[i].Type = foldUnions(f.Type, seenStructs, intersectStructs)
		}

	case UnionKind:
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		if len(elemTypes) == 0 {
			break
		}
		ts := make(typeset, len(elemTypes))
		for _, t := range elemTypes {
			ts.add(t)
		}
		if len(ts) == 0 {
			t.Desc = CompoundDesc{UnionKind, nil}
			return t
		}
		return foldUnionImpl(ts, seenStructs, intersectStructs)

	default:
		panic("Unknown noms kind")
	}
	return t
}

func foldUnionImpl(ts typeset, seenStructs typeset, intersectStructs bool) *Type {
	type how struct {
		k NomsKind
		n string
	}
	out := make(typeSlice, 0, len(ts))
	groups := map[how]typeset{}
	for t := range ts {
		var h how
		switch t.TargetKind() {
		case RefKind, SetKind, ListKind, MapKind:
			h = how{k: t.TargetKind()}
		case StructKind:
			h = how{k: t.TargetKind(), n: t.Desc.(StructDesc).Name}
		default:
			out = append(out, t)
			continue
		}
		g := groups[h]
		if g == nil {
			g = typeset{}
			groups[h] = g
		}
		g.add(t)
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
		case ListKind, RefKind, SetKind:
			r = foldCompoundTypesForUnion(h.k, ts, seenStructs, intersectStructs)
		case MapKind:
			r = foldMapTypesForUnion(ts, seenStructs, intersectStructs)
		case StructKind:
			r = foldStructTypes(h.n, ts, seenStructs, intersectStructs)
		}
		out = append(out, r)
	}

	for i, t := range out {
		out[i] = foldUnions(t, seenStructs, intersectStructs)
	}

	if len(out) == 1 {
		return out[0]
	}

	sort.Sort(out)

	return newType(CompoundDesc{UnionKind, out})
}

func foldCompoundTypesForUnion(k NomsKind, ts, seenStructs typeset, intersectStructs bool) *Type {
	elemTypes := make(typeset, len(ts))
	for t := range ts {
		d.PanicIfFalse(t.TargetKind() == k)
		elemTypes.add(t.Desc.(CompoundDesc).ElemTypes[0])
	}

	elemType := foldUnionImpl(elemTypes, seenStructs, intersectStructs)
	return makeCompoundType(k, elemType)
}

func foldMapTypesForUnion(ts, seenStructs typeset, intersectStructs bool) *Type {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for t := range ts {
		d.PanicIfFalse(t.TargetKind() == MapKind)
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		keyTypes.add(elemTypes[0])
		valTypes.add(elemTypes[1])
	}

	kt := foldUnionImpl(keyTypes, seenStructs, intersectStructs)
	vt := foldUnionImpl(valTypes, seenStructs, intersectStructs)

	return makeCompoundType(MapKind, kt, vt)
}

func foldStructTypesFieldsOnly(name string, ts, seenStructs typeset, intersectStructs bool) structTypeFields {
	fieldset := make([]structTypeFields, len(ts))
	i := 0
	for t := range ts {
		desc := t.Desc.(StructDesc)
		d.PanicIfFalse(desc.Name == name)
		fieldset[i] = desc.fields
		i++
	}

	return simplifyStructFields(fieldset, seenStructs, intersectStructs)
}

func foldStructTypes(name string, ts, seenStructs typeset, intersectStructs bool) *Type {
	fields := foldStructTypesFieldsOnly(name, ts, seenStructs, intersectStructs)
	return newType(StructDesc{name, fields})
}

func simplifyStructFields(in []structTypeFields, seenStructs typeset, intersectStructs bool) structTypeFields {
	// We gather all the fields/types into allFields. If the number of
	// times a field name is present is less that then number of types we
	// are simplifying then the field must be optional.
	// If we see an optional field we do not increment the count for it and
	// it will be treated as optional in the end.

	// If intersectStructs is true we need to pick the more restrictive version (n: T over n?: T).
	type fieldTypeInfo struct {
		anyNonOptional bool
		count          int
		ts             typeSlice
	}
	allFields := map[string]fieldTypeInfo{}

	for _, ff := range in {
		for _, f := range ff {
			fti, ok := allFields[f.Name]
			if !ok {
				fti = fieldTypeInfo{
					ts: make(typeSlice, 0, len(in)),
				}
			}
			fti.ts = append(fti.ts, f.Type)
			if !f.Optional {
				fti.count++
				fti.anyNonOptional = true
			}
			allFields[f.Name] = fti
		}
	}

	count := len(in)
	fields := make(structTypeFields, len(allFields))
	i := 0
	for name, fti := range allFields {
		nt := makeUnionType(fti.ts...)
		fields[i] = StructField{
			Name:     name,
			Type:     foldUnions(nt, seenStructs, intersectStructs),
			Optional: !(intersectStructs && fti.anyNonOptional) && fti.count < count,
		}
		i++
	}

	sort.Sort(fields)

	return fields
}
