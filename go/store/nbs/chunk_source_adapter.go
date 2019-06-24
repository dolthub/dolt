package nbs

type chunkSourceAdapter struct {
	tableReader
	h addr
}

func (csa chunkSourceAdapter) hash() addr {
	return csa.h
}

func (csa chunkSourceAdapter) index() tableIndex {
	return csa.tableIndex
}

func newReaderFromIndexData(indexCache *indexCache, idxData []byte, name addr, tra tableReaderAt, blockSize uint64) chunkSource {
	index := parseTableIndex(idxData)
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer indexCache.unlockEntry(name)
		indexCache.put(name, index)
	}
	return &chunkSourceAdapter{newTableReader(index, tra, blockSize), name}
}
