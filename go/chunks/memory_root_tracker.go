// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import "github.com/attic-labs/noms/go/hash"

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
