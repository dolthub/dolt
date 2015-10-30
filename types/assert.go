package types

import "github.com/attic-labs/noms/d"

func assertType(t TypeRef, v ...Value) {
	if t.Kind() != ValueKind {
		for _, v := range v {
			d.Chk.True(t.Equals(v.TypeRef()))
		}
	}
}

func assertSetsSameType(s Set, v ...Set) {
	if s.elemType().Kind() != ValueKind {
		t := s.TypeRef()
		for _, v := range v {
			d.Chk.True(t.Equals(v.TypeRef()))
		}
	}
}

func assertMapElemTypes(m Map, v ...Value) {
	elemTypes := m.elemTypes()
	keyType := elemTypes[0]
	valueType := elemTypes[0]
	if keyType.Kind() != ValueKind || valueType.Kind() != ValueKind {
		for i, v := range v {
			d.Chk.True(elemTypes[i%2].Equals(v.TypeRef()))
		}
	}
}
