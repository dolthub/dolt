// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

type tableManager interface {
	compact(mt *memTable, haver chunkReader) (name addr, chunkCount uint32)
	open(name addr, chunkCount uint32) chunkSource
}

type fileTableManager struct {
	dir string
}

func (ftm *fileTableManager) compact(mt *memTable, haver chunkReader) (name addr, chunkCount uint32) {
	return compact(ftm.dir, mt, haver)
}

func (ftm *fileTableManager) open(name addr, chunkCount uint32) chunkSource {
	return newMmapTableReader(ftm.dir, name, chunkCount)
}
