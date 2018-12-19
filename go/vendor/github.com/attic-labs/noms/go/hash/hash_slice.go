// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

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

func (rs HashSlice) HashSet() HashSet {
	hs := make(HashSet, len(rs))
	for _, h := range rs {
		hs[h] = struct{}{}
	}

	return hs
}
