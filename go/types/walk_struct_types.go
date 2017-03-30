// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// walkStructTypes walks a type hierarchy and calls a callback in post order
// for every struct type in that type tree.
// The callback may return a changed type and a new type tree will be created
// if that is the case.
// The callback is called with the type and a boolean which will be true if
// this is a cycle. If it is a cycle no more types are processed below it.
func walkStructTypes(tc *TypeCache, t *Type, parentStructTypes []*Type, cb func(*Type, bool) (*Type, bool)) (*Type, bool) {
	switch t.Kind() {
	case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind:
		return t, false
	case ListKind, MapKind, RefKind, SetKind, UnionKind:
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		changed := false
		newElemTypes := make(typeSlice, len(elemTypes))
		for i, et := range elemTypes {
			et2, c := walkStructTypes(tc, et, parentStructTypes, cb)
			newElemTypes[i] = et2
			changed = changed || c
		}
		if !changed {
			return t, changed
		}
		tc.Lock()
		defer tc.Unlock()
		return tc.getCompoundType(t.Kind(), newElemTypes...), true

	case StructKind:
		_, found := indexOfType(t, parentStructTypes)
		if found {
			return cb(t, true)
		}

		// Post order callback
		desc := t.Desc.(StructDesc)
		newFields := make(structFields, len(desc.fields))
		changed := false
		parentStructTypes = append(parentStructTypes, t)
		for i, f := range desc.fields {
			newType, c := walkStructTypes(tc, f.Type, parentStructTypes, cb)
			newFields[i] = StructField{Name: f.Name, Type: newType, Optional: f.Optional}
			changed = changed || c
		}

		newStructType := t
		if changed {
			func() {
				tc.Lock()
				defer tc.Unlock()
				newStructType = tc.makeStructTypeQuickly(desc.Name, newFields, checkKindNoValidate)
			}()
		}

		nt, c := cb(newStructType, false)
		return nt, c || changed

	case CycleKind:
		return t, false
	}

	panic("unreachable") // no more noms kinds
}
