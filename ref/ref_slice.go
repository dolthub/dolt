package ref

import (
	. "github.com/attic-labs/noms/dbg"
)

type RefSlice []Ref

func (rs RefSlice) Len() int {
	return len(rs)
}

func (rs RefSlice) Less(i, j int) bool {
	d1, d2 := rs[i].digest, rs[j].digest
	Chk.Equal(len(d1), len(d2))
	for k := 0; k < len(d1); k++ {
		b1, b2 := d1[k], d2[k]
		if b1 < b2 {
			return true
		} else if b1 > b2 {
			return false
		}
	}
	return false
}

func (rs RefSlice) Swap(i, j int) {
	t := rs[j]
	rs[j] = rs[i]
	rs[i] = t
}

func (rs RefSlice) Equals(other RefSlice) bool {
	if len(rs) != len(other) {
		return false
	}
	for i := 0; i < len(rs); i++ {
		if rs[i] != other[i] {
			return false
		}
	}
	return true
}
