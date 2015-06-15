package ref

type RefSlice []Ref

func (rs RefSlice) Len() int {
	return len(rs)
}

func (rs RefSlice) Less(i, j int) bool {
	return Less(rs[i], rs[j])
}

func (rs RefSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
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
