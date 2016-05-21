package hash

type HashSlice []Hash

func (rs HashSlice) Len() int {
	return len(rs)
}

func (rs HashSlice) Less(i, j int) bool {
	return rs[i].Less(rs[j])
}

func (rs HashSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func (rs HashSlice) Equals(other HashSlice) bool {
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
