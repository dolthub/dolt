package types

type Collection interface {
	Value
	Len() uint64
	Empty() bool
}

func valueSlicesEqual(v1 []Value, v2 []Value) bool {
	if len(v1) != len(v2) {
		return false
	}

	for i, _ := range v1 {
		if !v1[i].Equals(v2[i]) {
			return false
		}
	}

	return true
}
