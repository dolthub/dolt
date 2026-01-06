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
	"context"
	"io"
	"math"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func IterChunks(ctx context.Context, rd io.ReadSeeker, cb func(chunk chunks.Chunk) (stop bool, err error)) error {
	idx, err := readTableIndexByCopy(ctx, rd, &UnlimitedQuotaProvider{})
	if err != nil {
		return err
	}

	defer idx.Close()

	seen := make(map[hash.Hash]struct{})
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		ie, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}
		if _, ok := seen[h]; !ok {
			seen[h] = struct{}{}
			chunkBytes, err := readNFrom(rd, ie.Offset(), ie.Length())
			if err != nil {
				return err
			}

			cmpChnk, err := NewCompressedChunk(h, chunkBytes)
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

func GetTableIndexPrefixes(ctx context.Context, rd io.ReadSeeker) (prefixes []uint64, cleanup func(), err error) {
	idx, err := readTableIndexByCopy(ctx, rd, &UnlimitedQuotaProvider{})
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		cerr := idx.Close()
		if err == nil {
			err = cerr
		}
	}()

	prefixes, cleanup, err = idx.prefixes(ctx)
	if err != nil {
		return nil, nil, err
	}
	return
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

	buf := make([]byte, length)
	_, err = io.ReadFull(rd, buf)
	return buf, err
}
