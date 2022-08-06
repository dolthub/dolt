// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// IsSubtype determines whether concreteType is a subtype of requiredType. For example, `Float` is a subtype of `Float | String`.
func IsSubtype(nbf *NomsBinFormat, requiredType, concreteType *Type) bool {
	isSub, _ := isSubtypeTopLevel(nbf, requiredType, concreteType)
	return isSub
}

// IsSubtypeDisallowExtraFields is a slightly weird variant of IsSubtype. It returns true IFF IsSubtype(requiredType, concreteType) AND Structs in concreteType CANNOT have field names absent in requiredType
// ISSUE: https://github.com/attic-labs/noms/issues/3446
func IsSubtypeDisallowExtraStructFields(nbf *NomsBinFormat, requiredType, concreteType *Type) bool {
	isSub, hasExtra := isSubtypeDetails(nbf, requiredType, concreteType, false, nil)
	if hasExtra {
		return false
	}
	return isSub
}

// isSubtypeTopLevel returns two values: IsSub and hasExtra. See IsValueSubtypeOf()
// below for an explanation.
func isSubtypeTopLevel(nbf *NomsBinFormat, requiredType, concreteType *Type) (isSub bool, hasExtra bool) {
	return isSubtypeDetails(nbf, requiredType, concreteType, false, nil)
}

// IsSubtypeDetails returns two values:
//
//	isSub - which indicates whether concreteType is a subtype of requiredType.
//	hasExtra - which indicates whether concreteType has additional fields.
//
// See comment below on isValueSubtypeOfDetails
func isSubtypeDetails(nbf *NomsBinFormat, requiredType, concreteType *Type, hasExtra bool, parentStructTypes []*Type) (bool, bool) {
	if requiredType.Equals(concreteType) {
		return true, hasExtra
	}

	// If the concrete type is a union, all component types must be compatible.
	if concreteType.TargetKind() == UnionKind {
		for _, t := range concreteType.Desc.(CompoundDesc).ElemTypes {
			isSub, hasMore := isSubtypeDetails(nbf, requiredType, t, hasExtra, parentStructTypes)
			if !isSub {
				return false, hasExtra
			}
			hasExtra = hasExtra || hasMore
		}
		return true, hasExtra
	}

	// If the required type is a union, at least one of the component types must be compatible.
	if requiredType.TargetKind() == UnionKind {
		for _, t := range requiredType.Desc.(CompoundDesc).ElemTypes {
			isSub, hasMore := isSubtypeDetails(nbf, t, concreteType, hasExtra, parentStructTypes)
			if isSub {
				hasExtra = hasExtra || hasMore
				return true, hasExtra
			}
		}
		return false, hasExtra
	}

	if requiredType.TargetKind() != concreteType.TargetKind() {
		return requiredType.TargetKind() == ValueKind, hasExtra
	}

	if desc, ok := requiredType.Desc.(CompoundDesc); ok {
		concreteElemTypes := concreteType.Desc.(CompoundDesc).ElemTypes
		for i, t := range desc.ElemTypes {
			isSub, hasMore := compoundSubtype(nbf, t, concreteElemTypes[i], hasExtra, parentStructTypes)
			if !isSub {
				return false, hasExtra
			}
			hasExtra = hasExtra || hasMore
		}
		return true, hasExtra
	}

	if requiredType.TargetKind() == StructKind {
		requiredDesc := requiredType.Desc.(StructDesc)
		concreteDesc := concreteType.Desc.(StructDesc)
		if requiredDesc.Name != "" && requiredDesc.Name != concreteDesc.Name {
			return false, hasExtra
		}

		// We may already be computing the subtype for this type if we have a cycle.
		// In that case we exit the recursive check. We may still find that the type
		// is not a subtype but that will be handled at a higher level in the callstack.
		_, found := indexOfType(requiredType, parentStructTypes)
		if found {
			return true, hasExtra
		}

		i, j := 0, 0
		for i < requiredDesc.Len() && j < concreteDesc.Len() {
			requiredField := requiredDesc.fields[i]
			concreteField := concreteDesc.fields[j]
			if requiredField.Name == concreteField.Name {
				// Common field name
				if !requiredField.Optional && concreteField.Optional {
					return false, hasExtra
				}

				isSub, hasMore := isSubtypeDetails(nbf, requiredField.Type, concreteField.Type, hasExtra, append(parentStructTypes, requiredType))
				if !isSub {
					return false, hasExtra
				}
				hasExtra = hasExtra || hasMore

				i++
				j++
				continue
			}

			if requiredField.Name < concreteField.Name {
				// Concrete lacks field in required
				if !requiredField.Optional {
					return false, hasExtra
				}
				i++
			} else {
				// Concrete contains extra field
				hasExtra = true
				j++
			}
		}

		for i < requiredDesc.Len() {
			// Fields in required not in concrete
			if !requiredDesc.fields[i].Optional {
				hasExtra = true
				return false, hasExtra
			}
			i++
		}

		hasExtra = hasExtra || j < concreteDesc.Len()
		return true, hasExtra
	}

	panic("unreachable")
}

// compoundSubtype is called when comparing the element types of two compound types. This is the only case
// where a concrete type may have be a union type.
func compoundSubtype(nbf *NomsBinFormat, requiredType, concreteType *Type, hasExtra bool, parentStructTypes []*Type) (bool, bool) {
	// If the concrete type is a union then all the types in the union must be subtypes of the required typ. This also means that a compound type with an empty union is going to be a subtype of all compounds, List<> is a subtype of List<T> for all T.
	if concreteType.TargetKind() == UnionKind {
		for _, ct := range concreteType.Desc.(CompoundDesc).ElemTypes {
			isSub, hasExtra1 := isSubtypeDetails(nbf, requiredType, ct, hasExtra, parentStructTypes)
			if !isSub {
				return false, hasExtra1
			}
		}
		return true, hasExtra
	}
	return isSubtypeDetails(nbf, requiredType, concreteType, hasExtra, parentStructTypes)
}

func IsValueSubtypeOf(nbf *NomsBinFormat, v Value, t *Type) (bool, error) {
	isSub, _, err := isValueSubtypeOfDetails(nbf, v, t, false)

	if err != nil {
		return false, err
	}

	return isSub, nil
}

// IsValueSubtypeOfDetails returns two values:
//
//	isSub - which indicates whether v is a subtype of t.
//	hasExtra - which indicates whether v has additional fields. This field has
//	           no meaning if IsSub is false.
//
// For example, given the following data:
//
//	type1 := struct S {               v := Struct S1 {
//	    a Float | string                 a: "hello"
//	    b ?int                            b: 2
//	}                                 }
//
// IsValueSubtypeOfDetails(v, type1) would return isSub == true, and hasExtra == false
//
// And given these types:
//
//	type2 := struct S {               v := Struct S1 {
//	    a Float | string                 a: "hello"
//	    b ?int                            b: 2
//	}                                     c: "hello again"
//	                                  }
//
// IsValueSubtypeOfDetails(v, type1) would return isSub == true, and hasExtra == true
func IsValueSubtypeOfDetails(nbf *NomsBinFormat, v Value, t *Type) (bool, bool, error) {
	return isValueSubtypeOfDetails(nbf, v, t, false)
}

func isValueSubtypeOfDetails(nbf *NomsBinFormat, v Value, t *Type, hasExtra bool) (bool, bool, error) {
	switch t.TargetKind() {
	case ValueKind:
		return true, hasExtra, nil
	case UnionKind:
		var anonStruct *Type

		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			// Typically if IsSubtype(v.Type(), A|B|C|...) then exactly one of the
			// element types in the union will be a supertype of v.Type() because
			// of type simplification rules (only one of each kind is allowed in
			// the simplified union except for structs, where one of each unique
			// struct name is allowed).
			//
			// However there is one exception which is that type simplification
			// allows the struct with empty name. So if v.Type() is a struct with a
			// name, then it is possible for *two* elements in the union to match
			// it -- a struct with that same name, and a struct with no name.
			//
			// So if we happen across an element type that is an anonymous struct, we
			// save it for later and only try to use it if we can't find anything
			// better.
			if et.TargetKind() == StructKind && et.Desc.(StructDesc).Name == "" {
				anonStruct = et
				continue
			}
			isSub, hasMore, err := isValueSubtypeOfDetails(nbf, v, et, hasExtra)

			if err != nil {
				return false, false, err
			}

			if isSub {
				hasExtra = hasExtra || hasMore
				return isSub, hasExtra, nil
			}
		}

		if anonStruct != nil {
			isSub, hasMore, err := isValueSubtypeOfDetails(nbf, v, anonStruct, hasExtra)

			if err != nil {
				return false, false, err
			}

			if isSub {
				hasExtra = hasExtra || hasMore
				return isSub, hasExtra, nil
			}
		}

		return false, hasExtra, nil
	case CycleKind:
		panic("unreachable") // CycleKind are ephemeral.
	default:
		if IsPrimitiveKind(t.TargetKind()) {
			return v.Kind() == t.TargetKind(), hasExtra, nil
		}
		if v.Kind() != t.TargetKind() {
			return false, hasExtra, nil
		}
	}

	switch desc := t.Desc.(type) {
	case StructDesc:
		// If we provide a named struct type we require that the names match.
		s := v.(Struct)
		if desc.Name != "" && desc.Name != s.Name() {
			return false, hasExtra, nil
		}
		missingOptionalFieldCnt := 0
		for _, f := range desc.fields {
			fv, ok, err := s.MaybeGet(f.Name)

			if err != nil {
				return false, false, err
			}

			if !ok {
				if f.Optional {
					missingOptionalFieldCnt += 1
				} else {
					return false, hasExtra, nil
				}
			} else {
				isSub, hasMore, err := isValueSubtypeOfDetails(nbf, fv, f.Type, hasExtra)

				if err != nil {
					return false, false, err
				}

				if !isSub {
					return false, hasExtra, nil
				}
				hasExtra = hasExtra || hasMore
			}
		}
		if s.Len()+missingOptionalFieldCnt > len(desc.fields) {
			hasExtra = true
		}
		return true, hasExtra, nil

	case CompoundDesc:
		switch v := v.(type) {
		case Ref:
			// Switching to the type is subtype of type here.
			t, err := v.TargetType()

			if err != nil {
				return false, false, err
			}
			isSub, hasExtra := isSubtypeTopLevel(nbf, desc.ElemTypes[0], t)
			return isSub, hasExtra, nil
		case Map:
			kt := desc.ElemTypes[0]
			vt := desc.ElemTypes[1]
			if seq, ok := v.orderedSequence.(mapLeafSequence); ok {
				meSl, err := seq.entries()

				if err != nil {
					return false, false, err
				}

				for _, entry := range meSl.entries {
					isSub, hasMore, err := isValueSubtypeOfDetails(nbf, entry.key, kt, hasExtra)

					if err != nil {
						return false, false, err
					}

					if !isSub {
						return false, hasExtra, nil
					}

					hasExtra = hasExtra || hasMore
					isSub, hasExtra, err = isValueSubtypeOfDetails(nbf, entry.value, vt, hasExtra)

					if err != nil {
						return false, false, err
					}

					if !isSub {
						return false, hasExtra, nil
					}
					hasExtra = hasExtra || hasMore
				}
				return true, hasExtra, nil
			}
			return isMetaSequenceSubtypeOf(nbf, v.orderedSequence.(metaSequence), t, hasExtra)
		case Set:
			et := desc.ElemTypes[0]
			if seq, ok := v.orderedSequence.(setLeafSequence); ok {
				vals, err := seq.values()

				if err != nil {
					return false, false, err
				}
				for _, v := range vals {
					isSub, hasMore, err := isValueSubtypeOfDetails(nbf, v, et, hasExtra)

					if err != nil {
						return false, false, err
					}

					if !isSub {
						return false, hasExtra, nil
					}
					hasExtra = hasExtra || hasMore
				}
				return true, hasExtra, nil
			}
			return isMetaSequenceSubtypeOf(nbf, v.orderedSequence.(metaSequence), t, hasExtra)
		case List:
			et := desc.ElemTypes[0]
			if seq, ok := v.sequence.(listLeafSequence); ok {
				vals, err := seq.values()

				if err != nil {
					return false, false, err
				}

				for _, v := range vals {
					isSub, hasMore, err := isValueSubtypeOfDetails(nbf, v, et, hasExtra)

					if err != nil {
						return false, false, err
					}

					if !isSub {
						return false, hasExtra, nil
					}
					hasExtra = hasExtra || hasMore
				}
				return true, hasExtra, nil
			}
			return isMetaSequenceSubtypeOf(nbf, v.sequence.(metaSequence), t, hasExtra)
		}
	}
	panic("unreachable")
}

func isMetaSequenceSubtypeOf(nbf *NomsBinFormat, ms metaSequence, t *Type, hasExtra bool) (bool, bool, error) {
	// TODO: iterRefs
	tups, err := ms.tuples()

	if err != nil {
		return false, false, err
	}
	for _, mt := range tups {
		ref, err := mt.ref()

		if err != nil {
			return false, false, err
		}

		tt, err := ref.TargetType()

		if err != nil {
			return false, false, err
		}

		// Each prolly tree is also a List<T> where T needs to be a subtype.
		isSub, hasMore := isSubtypeTopLevel(nbf, t, tt)
		if !isSub {
			return false, hasExtra, nil
		}
		hasExtra = hasExtra || hasMore
	}
	return true, hasExtra, nil
}
