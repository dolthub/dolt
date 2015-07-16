package chunks

import "github.com/attic-labs/noms/ref"

type memoryRootTracker ref.Ref

func (ms *memoryRootTracker) Root() ref.Ref {
	return ref.Ref(*ms)
}

func (ms *memoryRootTracker) UpdateRoot(current, last ref.Ref) bool {
	if last != ref.Ref(*ms) {
		return false
	}

	*ms = memoryRootTracker(current)
	return true
}
