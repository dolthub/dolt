// Copyright 2021 Dolthub, Inc.
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

import (
	"io"
	"math"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func IterChunks(rd io.ReadSeeker, cb func(chunk chunks.Chunk) (stop bool, err error)) error {
	idx, err := ReadTableIndexByCopy(rd, &noopQuotaProvider{})
	if err != nil {
		return err
	}

	defer idx.Close()

	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var a addr
		ie, err := idx.IndexEntry(i, &a)
		if err != nil {
			return err
		}
		if _, ok := seen[a]; !ok {
			seen[a] = true
			chunkBytes, err := readNFrom(rd, ie.Offset(), ie.Length())
			if err != nil {
				return err
			}

			cmpChnk, err := NewCompressedChunk(hash.Hash(a), chunkBytes)
			if err != nil {
				return err
			}

			chunk, err := cmpChnk.ToChunk()
			if err != nil {
				return err
			}

			stop, err := cb(chunk)
			if err != nil {
				return err
			} else if stop {
				break
			}
		}
	}

	return nil
}

func GetTableIndexPrefixes(rd io.ReadSeeker) (prefixes []uint64, err error) {
	idx, err := ReadTableIndexByCopy(rd, &noopQuotaProvider{})
	if err != nil {
		return nil, err
	}
	defer func() {
		cerr := idx.Close()
		if err == nil {
			err = cerr
		}
	}()

	return idx.Prefixes()
}

func GuessPrefixOrdinal(prefix uint64, n uint32) int {
	hi := prefix >> 32
	return int((hi * uint64(n)) / uint64(math.MaxUint32))
}

func readNFrom(rd io.ReadSeeker, offset uint64, length uint32) ([]byte, error) {
	_, err := rd.Seek(int64(offset), io.SeekStart)

	if err != nil {
		return nil, err
	}

	return iohelp.ReadNBytes(rd, int(length))
}
