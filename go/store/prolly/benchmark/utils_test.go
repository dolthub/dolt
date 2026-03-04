// Copyright 2022 Dolthub, Inc.
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
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyBench struct {
	m    prolly.Map
	tups [][2]val.Tuple
}

type bboltBench struct {
	db   *bbolt.DB
	tups [][2]val.Tuple
}

func generateProllyBench(b *testing.B, size uint64) prollyBench {
	b.StopTimer()
	defer b.StartTimer()
	ctx := context.Background()
	ns := newTestNodeStore()

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	tups := generateProllyTuples(kd, vd, size, ns)

	tt := make([]val.Tuple, 0, len(tups)*2)
	for i := range tups {
		tt = append(tt, tups[i][0], tups[i][1])
	}

	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd, tt...)
	if err != nil {
		panic(err)
	}

	return prollyBench{m: m, tups: tups}
}

var shared = pool.NewBuffPool()

func newTestNodeStore() tree.NodeStore {
	ts := &chunks.TestStorage{}
	return tree.NewNodeStore(ts.NewView())
}

func generateProllyTuples(kd, vd *val.TupleDesc, size uint64, ns tree.NodeStore) [][2]val.Tuple {
	src := rand.NewSource(0)

	tups := make([][2]val.Tuple, size)
	kb := val.NewTupleBuilder(kd, ns)
	vb := val.NewTupleBuilder(vd, ns)

	var err error
	for i := range tups {
		// key
		kb.PutUint64(0, uint64(i))
		tups[i][0], err = kb.Build(shared)
		if err != nil {
			panic(err)
		}

		// val
		vb.PutInt64(0, src.Int63())
		vb.PutInt64(1, src.Int63())
		vb.PutInt64(2, src.Int63())
		vb.PutInt64(3, src.Int63())
		vb.PutInt64(4, src.Int63())
		tups[i][1], err = vb.Build(shared)
		if err != nil {
			panic(err)
		}
	}

	return tups
}

func generateBBoltBench(b *testing.B, size uint64) bboltBench {
	b.StopTimer()
	defer b.StartTimer()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

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

	tups := generateProllyTuples(kd, vd, size, nil)

	const batch = 4096
	for i := 0; i < len(tups); i += batch {
		err = db.Update(func(tx *bbolt.Tx) error {
			bck := tx.Bucket(bucket)
			for j := i; j < (i+batch) && j < len(tups); j++ {
				require.NoError(b, bck.Put(tups[j][0], tups[j][1]))
			}
			return nil
		})
		require.NoError(b, err)
	}
	return bboltBench{db: db, tups: tups}
}
