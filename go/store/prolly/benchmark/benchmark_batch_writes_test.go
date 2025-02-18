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

package benchmark

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

const (
	batch = 1 << 16
	sz    = 8
)

var (
	bucket = []byte("bolt")
)

func BenchmarkImportBBolt(b *testing.B) {
	makeWriter := func() writer {
		path, err := os.MkdirTemp("", "*")
		require.NoError(b, err)
		path = filepath.Join(path, "bolt.db")

		db, err := bbolt.Open(path, 0666, &bbolt.Options{
			// turn off fsync
			NoGrowSync:     true,
			NoFreelistSync: true,
			NoSync:         true,
		})
		require.NoError(b, err)

		err = db.Update(func(tx *bbolt.Tx) error {
			_, err = tx.CreateBucket(bucket)
			return err
		})
		require.NoError(b, err)
		return &bboltWriter{
			edits: skip.NewSkipList(bytes.Compare),
			db:    db,
		}
	}

	b.Run("BBolt", func(b *testing.B) {
		benchmarkBatchWrite(b, makeWriter())
	})
}

func BenchmarkImportDolt(b *testing.B) {
	makeWriter := func() writer {
		ctx := context.Background()
		nbf := types.Format_DOLT
		memtable := uint64(256 * 1024 * 1024)
		quota := nbs.NewUnlimitedMemQuotaProvider()

		path, err := os.MkdirTemp("", "*")
		require.NoError(b, err)

		cs, err := nbs.NewLocalStore(ctx, nbf.VersionString(), path, memtable, quota)
		require.NoError(b, err)

		desc := val.NewTupleDescriptor(val.Type{Enc: val.Uint64Enc})
		m, err := prolly.NewMapFromTuples(ctx, tree.NewNodeStore(cs), desc, desc)
		require.NoError(b, err)
		return &doltWriter{mut: m.Mutate(), cs: cs}
	}

	b.Run("Dolt", func(b *testing.B) {
		benchmarkBatchWrite(b, makeWriter())
	})
}

type bboltWriter struct {
	edits *skip.List
	db    *bbolt.DB
}

func (wr *bboltWriter) Put(ctx context.Context, key, value []byte) error {
	wr.edits.Put(ctx, key, value)
	return nil
}

func (wr *bboltWriter) Flush() error {
	return wr.db.Update(func(tx *bbolt.Tx) (err error) {
		b := tx.Bucket(bucket)
		iter := wr.edits.IterAtStart()
		for {
			k, v := iter.Current()
			if k == nil {
				break
			}
			if err = b.Put(k, v); err != nil {
				return
			}
			iter.Advance()
		}
		return
	})
}

type doltWriter struct {
	mut *prolly.MutableMap
	cs  *nbs.NomsBlockStore
}

func (wr *doltWriter) Put(ctx context.Context, key, value []byte) error {
	return wr.mut.Put(context.Background(), key, value)
}

func (wr *doltWriter) Flush() error {
	m, err := wr.mut.Map(context.Background())
	if err != nil {
		return err
	}
	wr.mut = m.Mutate()

	h, err := wr.cs.Root(context.Background())
	if err != nil {
		return err
	}
	_, err = wr.cs.Commit(context.Background(), h, h)
	return err
}

func benchmarkBatchWrite(b *testing.B, wr writer) {
	dp := newDataProvider(batch)
	for i := 0; i < b.N; i++ {
		k, v := dp.next()
		require.NoError(b, wr.Put(ctx, k, v))
		if dp.empty() {
			require.NoError(b, wr.Flush())
			dp = newDataProvider(batch)
		}
	}
}

type writer interface {
	Put(ctx context.Context, key, value []byte) error
	Flush() error
}

type dataProvider struct {
	buf []byte
}

var _ sort.Interface = &dataProvider{}

func newDataProvider(count int) (dp *dataProvider) {
	dp = &dataProvider{buf: make([]byte, count*sz)}
	rand.Read(dp.buf)
	return
}

func (dp *dataProvider) next() (k, v []byte) {
	k, v = dp.buf[:sz], dp.buf[:sz]
	dp.buf = dp.buf[sz:]
	return
}

func (dp *dataProvider) empty() bool {
	return len(dp.buf) == 0
}

func (dp *dataProvider) Len() int {
	return len(dp.buf) / sz
}

func (dp *dataProvider) Less(i, j int) bool {
	l := dp.buf[i*sz : (i*sz)+sz]
	r := dp.buf[j*sz : (j*sz)+sz]
	return bytes.Compare(l, r) < 0
}

var swap [sz]byte

func (dp *dataProvider) Swap(i, j int) {
	l := dp.buf[i*sz : (i*sz)+sz]
	r := dp.buf[j*sz : (j*sz)+sz]
	copy(swap[:], l)
	copy(l, r)
	copy(r, swap[:])
}
