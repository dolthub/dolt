package ase

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
)

type KVPIterator interface {
	Next() *KVP
}

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

func IsInOrder(itr KVPIterator) (bool, int) {
	count := 1
	prev := itr.Next()

	for {
		var curr *KVP
		curr = itr.Next()

		if curr == nil {
			break
		} else if !prev.Key.Less(curr.Key) && !prev.Key.Equals(curr.Key) {
			fmt.Println(types.EncodedValue(prev.Key), ">=", types.EncodedValue(curr.Key))
			return false, count
		}

		count++
		prev = curr
	}

	return true, count
}
