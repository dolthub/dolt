// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

func assertSubtype(t *Type, v Value) {
	if !isSubtype(t, v.Type()) {
		d.Chk.Fail("Invalid type")
	}
}

func isSubtype(requiredType, concreteType *Type) bool {
	if requiredType.Equals(concreteType) {
		return true
	}

	if requiredType.Kind() == UnionKind {
		for _, t := range requiredType.Desc.(CompoundDesc).ElemTypes {
			if isSubtype(t, concreteType) {
				return true
			}
		}
		return false
	}

	if requiredType.Kind() != concreteType.Kind() {
		return requiredType.Kind() == ValueKind
	}

	if desc, ok := requiredType.Desc.(CompoundDesc); ok {
		concreteElemTypes := concreteType.Desc.(CompoundDesc).ElemTypes
		for i, t := range desc.ElemTypes {
			if !compoundSubtype(t, concreteElemTypes[i]) {
				return false
			}
		}
		return true
	}

	if requiredType.Kind() == StructKind {
		requiredDesc := requiredType.Desc.(StructDesc)
		concreteDesc := concreteType.Desc.(StructDesc)
		if requiredDesc.Name != "" && requiredDesc.Name != concreteDesc.Name {
			return false
		}
		j := 0
		for _, field := range requiredDesc.fields {
			for ; j < concreteDesc.Len() && concreteDesc.fields[j].name != field.name; j++ {
			}
			if j == concreteDesc.Len() {
				return false
			}

			f := concreteDesc.fields[j]
			if !isSubtype(field.t, f.t) {
				return false
			}
		}
		return true

	}

	panic("unreachable")
}

func compoundSubtype(requiredType, concreteType *Type) bool {
	// In a compound type it is OK to have an empty union.
	if concreteType.Kind() == UnionKind && len(concreteType.Desc.(CompoundDesc).ElemTypes) == 0 {
		return true
	}
	return isSubtype(requiredType, concreteType)
}
