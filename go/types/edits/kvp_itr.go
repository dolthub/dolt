package edits

import "github.com/attic-labs/noms/go/types"

// IsInOrder iterates over every value and validates that they are returned in key order.  This is intended for testing.
func IsInOrder(itr types.EditProvider) (bool, int) {
	prev := itr.Next()

	if prev == nil {
		return true, 0
	}

	count := 1

	for {
		var curr *types.KVP
		curr = itr.Next()

		if curr == nil {
			break
		} else if curr.Key.Less(prev.Key) {
			return false, count
		}

		count++
		prev = curr
	}

	return true, count
}
