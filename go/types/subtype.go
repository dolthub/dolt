// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
)

func assertSubtype(t *Type, v Value) {
	if !IsValueSubtypeOf(v, t) {
		d.Panic("Invalid type. %s is not a subtype of %s", TypeOf(v).Describe(), t.Describe())
	}
}

// IsSubtype determines whether concreteType is a subtype of requiredType. For example, `Number` is a subtype of `Number | String`.
func IsSubtype(requiredType, concreteType *Type) bool {
	return isSubtype(requiredType, concreteType, nil)
}

func isSubtype(requiredType, concreteType *Type, parentStructTypes []*Type) bool {
	if requiredType.Equals(concreteType) {
		return true
	}

	// If the concrete type is a union, all component types must be compatible.
	if concreteType.TargetKind() == UnionKind {
		for _, t := range concreteType.Desc.(CompoundDesc).ElemTypes {
			if !isSubtype(requiredType, t, parentStructTypes) {
				return false
			}
		}
		return true
	}

	// If the required type is a union, at least one of the component types must be compatible.
	if requiredType.TargetKind() == UnionKind {
		for _, t := range requiredType.Desc.(CompoundDesc).ElemTypes {
			if isSubtype(t, concreteType, parentStructTypes) {
				return true
			}
		}
		return false
	}

	if requiredType.TargetKind() != concreteType.TargetKind() {
		return requiredType.TargetKind() == ValueKind
	}

	if desc, ok := requiredType.Desc.(CompoundDesc); ok {
		concreteElemTypes := concreteType.Desc.(CompoundDesc).ElemTypes
		for i, t := range desc.ElemTypes {
			if !compoundSubtype(t, concreteElemTypes[i], parentStructTypes) {
				return false
			}
		}
		return true
	}

	if requiredType.TargetKind() == StructKind {
		requiredDesc := requiredType.Desc.(StructDesc)
		concreteDesc := concreteType.Desc.(StructDesc)
		if requiredDesc.Name != "" && requiredDesc.Name != concreteDesc.Name {
			return false
		}

		// We may already be computing the subtype for this type if we have a cycle.
		// In that case we exit the recursive check. We may still find that the type
		// is not a subtype but that will be handled at a higher level in the callstack.
		_, found := indexOfType(requiredType, parentStructTypes)
		if found {
			return true
		}

		j := 0
	outer:
		for _, requiredField := range requiredDesc.fields {
			for {
				if j >= concreteDesc.Len() {
					if requiredField.Optional {
						continue outer
					}
					return false
				}
				concreteField := concreteDesc.fields[j]

				if concreteField.Name == requiredField.Name {
					if requiredField.Optional {
						break
					}
					if concreteField.Optional {
						return false
					}
					break
				}
				if requiredField.Optional && concreteField.Name > requiredField.Name {
					continue outer
				}
				j++
			}

			concreteField := concreteDesc.fields[j]
			if !isSubtype(requiredField.Type, concreteField.Type, append(parentStructTypes, requiredType)) {
				return false
			}
		}
		return true

	}

	panic("unreachable")
}

// compoundSubtype is called when comparing the element types of two compound types. This is the only case
// where a concrete type may have be a union type.
func compoundSubtype(requiredType, concreteType *Type, parentStructTypes []*Type) bool {
	// If the concrete type is a union then all the types in the union must be subtypes of the required typ. This also means that a compound type with an empty union is going to be a subtype of all compounds, List<> is a subtype of List<T> for all T.
	if concreteType.TargetKind() == UnionKind {
		for _, ct := range concreteType.Desc.(CompoundDesc).ElemTypes {
			if !isSubtype(requiredType, ct, parentStructTypes) {
				return false
			}
		}
		return true
	}
	return isSubtype(requiredType, concreteType, parentStructTypes)
}

// IsValueSubtypeOf returns whether a value is a subtype of a type.
func IsValueSubtypeOf(v Value, t *Type) bool {
	switch t.TargetKind() {
	case BoolKind, NumberKind, StringKind, BlobKind, TypeKind:
		return v.Kind() == t.TargetKind()
	case ValueKind:
		return true
	case UnionKind:
		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			if IsValueSubtypeOf(v, et) {
				return true
			}
		}
		return false
	case CycleKind:
		panic("unreachable") // CycleKind are ephemeral.
	default:
		if v.Kind() != t.TargetKind() {
			return false
		}
	}

	switch desc := t.Desc.(type) {
	case StructDesc:
		// If we provide a named struct type we require that the names match.
		s := v.(Struct)
		if desc.Name != "" && desc.Name != s.Name() {
			return false
		}

		for _, f := range desc.fields {
			fv, ok := s.MaybeGet(f.Name)
			if (!ok && !f.Optional) || (ok && !IsValueSubtypeOf(fv, f.Type)) {
				return false
			}
		}
		return true

	case CompoundDesc:
		switch v := v.(type) {
		case Ref:
			// Switching to the type is subtype of type here.
			return isSubtype(desc.ElemTypes[0], v.TargetType(), nil)
		case Map:
			kt := desc.ElemTypes[0]
			vt := desc.ElemTypes[1]
			if seq, ok := v.seq.(mapLeafSequence); ok {
				for _, entry := range seq.data {
					if !IsValueSubtypeOf(entry.key, kt) {
						return false
					}
					if !IsValueSubtypeOf(entry.value, vt) {
						return false
					}
				}
				return true
			}
			return isMetaSequenceSubtypeOf(v.seq.(metaSequence), t)
		case Set:
			et := desc.ElemTypes[0]
			if seq, ok := v.seq.(setLeafSequence); ok {
				for _, v := range seq.data {
					if !IsValueSubtypeOf(v, et) {
						return false
					}
				}
				return true
			}
			return isMetaSequenceSubtypeOf(v.seq.(metaSequence), t)
		case List:
			et := desc.ElemTypes[0]
			if seq, ok := v.seq.(listLeafSequence); ok {
				for _, v := range seq.values {
					if !IsValueSubtypeOf(v, et) {
						return false
					}
				}
				return true
			}
			return isMetaSequenceSubtypeOf(v.seq.(metaSequence), t)
		}
	}
	panic("unreachable")
}

func isMetaSequenceSubtypeOf(ms metaSequence, t *Type) bool {
	for _, mt := range ms.tuples {
		// Each prolly tree is also a List<T> where T needs to be a subtype.
		if !isSubtype(t, mt.ref.TargetType(), nil) {
			return false
		}
	}
	return true
}
