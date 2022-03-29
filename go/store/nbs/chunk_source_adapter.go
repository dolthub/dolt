// Copyright 2019 Dolthub, Inc.
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

func newReaderFromIndexData(q MemoryQuotaProvider, idxData []byte, name addr, tra tableReaderAt, blockSize uint64) (cs chunkSource, err error) {
	index, err := parseTableIndexByCopy(idxData, q)
	if err != nil {
		return nil, err
	}

	tr, err := newTableReader(index, tra, blockSize)
	if err != nil {
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

func (csa chunkSourceAdapter) Close() error {
	return csa.tableReader.Close()
}

func (csa chunkSourceAdapter) Clone() (chunkSource, error) {
	tr, err := csa.tableReader.Clone()
	if err != nil {
		return &chunkSourceAdapter{}, err
	}
	return &chunkSourceAdapter{tr, csa.h}, nil
}
