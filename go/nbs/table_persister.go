// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

type tablePersister interface {
	Compact(mt *memTable, haver chunkReader) chunkSource
	Open(name addr, chunkCount uint32) chunkSource
}
