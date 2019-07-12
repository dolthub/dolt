package nbs

type chunkSourceAdapter struct {
	tableReader
	h addr
}

func (csa chunkSourceAdapter) hash() (addr, error) {
	return csa.h, nil
}

func (csa chunkSourceAdapter) index() (tableIndex, error) {
	return csa.tableIndex, nil
}

func newReaderFromIndexData(indexCache *indexCache, idxData []byte, name addr, tra tableReaderAt, blockSize uint64) (cs chunkSource, err error) {
	index, err := parseTableIndex(idxData)

	if err != nil {
		return nil, err
	}

	if indexCache != nil {
		indexCache.lockEntry(name)
		defer func() {
			unlockErr := indexCache.unlockEntry(name)

			if err == nil {
				err = unlockErr
			}
		}()
		indexCache.put(name, index)
	}

	return &chunkSourceAdapter{newTableReader(index, tra, blockSize), name}, nil
}
