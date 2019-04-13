package ase

import "github.com/attic-labs/noms/go/types"

type KVP struct {
	Key types.Value
	Val types.Value
}

type KVPSlice []KVP

func (kvps KVPSlice) Len() int {
	return len(kvps)
}

func (kvps KVPSlice) Less(i, j int) bool {
	return kvps[i].Key.Less(kvps[j].Key)
}

func (kvps KVPSlice) Swap(i, j int) {
	kvps[i], kvps[j] = kvps[j], kvps[i]
}

func (kvps KVPSlice) Merge(other KVPSlice) KVPSlice {
	i := 0
	j := 0
	k := 0

	x := kvps[i]
	y := other[j]
	dest := make(KVPSlice, len(kvps)+len(other))

	for {
		if x.Key.Less(y.Key) {
			dest[k] = x
			k++
			i++

			if i < len(kvps) {
				x = kvps[i]
			} else {
				copy(dest[k:], other[j:])
				break
			}
		} else {
			dest[k] = y
			k++
			j++

			if j < len(other) {
				y = other[j]
			} else {
				copy(dest[k:], kvps[i:])
				break
			}
		}
	}

	return dest
}
