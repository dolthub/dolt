package chunks

import "github.com/attic-labs/noms/hash"

type memoryRootTracker hash.Hash

func (ms *memoryRootTracker) Root() hash.Hash {
	return hash.Hash(*ms)
}

func (ms *memoryRootTracker) UpdateRoot(current, last hash.Hash) bool {
	if last != hash.Hash(*ms) {
		return false
	}

	*ms = memoryRootTracker(current)
	return true
}
