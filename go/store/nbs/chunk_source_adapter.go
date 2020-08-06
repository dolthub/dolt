// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

type chunkSourceAdapter struct {
	tableReader
	h addr
}

func (csa chunkSourceAdapter) hash() (addr, error) {
	return csa.h, nil
}

func (csa chunkSourceAdapter) index() (onHeapTableIndex, error) {
	return csa.tableReader.index()
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
