package types

import "github.com/attic-labs/noms/d"

func assertType(t *Type, v Value) {
	if !subtype(t, v.Type()) {
		d.Chk.Fail("Invalid type", "Expected: %s, found: %s", t.Describe(), v.Type().Describe())
	}
}

func subtype(expected, actual *Type) bool {
	if expected.Equals(actual) {
		return true
	}

	if expected.Kind() == UnionKind {
		for _, t := range expected.Desc.(CompoundDesc).ElemTypes {
			if subtype(t, actual) {
				return true
			}
		}
		return false
	}

	if expected.Kind() != actual.Kind() {
		return expected.Kind() == ValueKind
	}

	if desc, ok := expected.Desc.(CompoundDesc); ok {
		actualElemTypes := actual.Desc.(CompoundDesc).ElemTypes
		for i, t := range desc.ElemTypes {
			if !compoundSubtype(t, actualElemTypes[i]) {
				return false
			}
		}
		return true
	}

	panic("unreachable")
}

func compoundSubtype(expected, actual *Type) bool {
	// In a compound type it is OK to have an empty union.
	if actual.Kind() == UnionKind && len(actual.Desc.(CompoundDesc).ElemTypes) == 0 {
		return true
	}
	return subtype(expected, actual)
}
