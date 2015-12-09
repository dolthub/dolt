package types

import "github.com/attic-labs/noms/d"

func assertType(t Type, v ...Value) {
	if t.Kind() != ValueKind {
		for _, v := range v {
			d.Chk.True(t.Equals(v.Type()), "Invalid type. Expected: %s, found: %s", t.Describe(), v.Type().Describe())
		}
	}
}

func assertSetsSameType(s setLeaf, v ...Set) {
	if s.elemType().Kind() != ValueKind {
		t := s.Type()
		for _, v := range v {
			d.Chk.True(t.Equals(v.Type()))
		}
	}
}

func assertMapElemTypes(m mapLeaf, v ...Value) {
	elemTypes := m.elemTypes()
	keyType := elemTypes[0]
	valueType := elemTypes[0]
	if keyType.Kind() != ValueKind || valueType.Kind() != ValueKind {
		for i, v := range v {
			d.Chk.True(elemTypes[i%2].Equals(v.Type()))
		}
	}
}
