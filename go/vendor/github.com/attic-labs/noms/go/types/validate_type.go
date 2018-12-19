// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

func validateType(t *Type) {
	validateTypeImpl(t, map[string]struct{}{})
}

func validateTypeImpl(t *Type, seenStructs map[string]struct{}) {
	switch desc := t.Desc.(type) {
	case CompoundDesc:
		if desc.Kind() == UnionKind {
			if len(desc.ElemTypes) == 1 {
				panic("Invalid union type")
			}
			for i := 1; i < len(desc.ElemTypes); i++ {
				if !unionLess(desc.ElemTypes[i-1], desc.ElemTypes[i]) {
					panic("Invalid union order")
				}
			}
		}

		for _, et := range desc.ElemTypes {
			validateTypeImpl(et, seenStructs)
		}
	case StructDesc:
		if desc.Name != "" {
			if _, ok := seenStructs[desc.Name]; ok {
				return
			}
			seenStructs[desc.Name] = struct{}{}
		}
		verifyStructName(desc.Name)
		verifyFields(desc.fields)
		for _, f := range desc.fields {
			validateTypeImpl(f.Type, seenStructs)
		}
	}
}
