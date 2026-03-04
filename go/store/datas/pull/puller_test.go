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

package pull

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
	"github.com/dolthub/dolt/go/store/val"
)

func TestNbsPuller(t *testing.T) {
	testPuller(t, func(ctx context.Context) (tree.NodeStore, datas.Database) {
		dir := filepath.Join(os.TempDir(), uuid.New().String())
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		nbf := types.Format_Default.VersionString()
		q := nbs.NewUnlimitedMemQuotaProvider()
		st, err := nbs.NewLocalStore(ctx, nbf, dir, clienttest.DefaultMemTableSize, q, false)
		require.NoError(t, err)

		ns := tree.NewNodeStore(st)
		db := datas.NewDatabase(st)
		return ns, db
	})
}

func TestChunkJournalPuller(t *testing.T) {
	testPuller(t, func(ctx context.Context) (tree.NodeStore, datas.Database) {
		dir := filepath.Join(os.TempDir(), uuid.New().String())
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		nbf := types.Format_Default.VersionString()
		q := nbs.NewUnlimitedMemQuotaProvider()

		st, err := nbs.NewLocalJournalingStore(ctx, nbf, dir, q, false, nil)
		require.NoError(t, err)

		ns := tree.NewNodeStore(st)
		db := datas.NewDatabase(st)
		return ns, db
	})
}

func TestPuller(t *testing.T) {
	t.Run("GhostChunk", func(t *testing.T) {
		ctx := context.Background()
		gs, err := nbs.NewGhostBlockStore(t.TempDir())
		waf, err := types.WalkAddrsForChunkStore(gs)
		require.NoError(t, err)

		ghost := hash.Parse("e6esqr35dkqnc7updhj6ap5v82sahm9r")

		gs.PersistGhostHashes(ctx, hash.NewHashSet(ghost))

		statsCh := make(chan Stats)
		go func() {
			for _ = range statsCh {
			}
		}()

		dir := filepath.Join(os.TempDir(), uuid.New().String())
		err = os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		nbf := types.Format_Default.VersionString()
		q := nbs.NewUnlimitedMemQuotaProvider()

		st, err := nbs.NewLocalJournalingStore(ctx, nbf, dir, q, false, nil)
		require.NoError(t, err)

		plr, err := NewPuller(ctx, t.TempDir(), 1<<20, gs, st, waf, []hash.Hash{ghost}, statsCh)
		require.NoError(t, err)
		err = plr.Pull(ctx)
		require.ErrorIs(t, err, nbs.ErrGhostChunkRequested)
	})
}

var (
	testKeyDesc = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false})
	testValDesc = val.NewTupleDescriptor(
		val.Type{Enc: val.StringEnc, Nullable: true},
		val.Type{Enc: val.StringEnc, Nullable: true},
	)
)

type testTuple struct {
	key int64
	v0  string
	v1  string
}

func buildKeyTuple(ns tree.NodeStore, k int64) val.Tuple {
	tb := val.NewTupleBuilder(testKeyDesc, ns)
	tb.PutInt64(0, k)
	tup, _ := tb.Build(ns.Pool())
	return tup
}

func buildValTuple(ns tree.NodeStore, v0, v1 string) val.Tuple {
	tb := val.NewTupleBuilder(testValDesc, ns)
	tb.PutString(0, v0)
	tb.PutString(1, v1)
	tup, _ := tb.Build(ns.Pool())
	return tup
}

func createProllyMap(t *testing.T, ctx context.Context, ns tree.NodeStore, data []testTuple) prolly.Map {
	tuples := make([]val.Tuple, 0, len(data)*2)
	for _, p := range data {
		tuples = append(tuples, buildKeyTuple(ns, p.key), buildValTuple(ns, p.v0, p.v1))
	}
	m, err := prolly.NewMapFromTuples(ctx, ns, testKeyDesc, testValDesc, tuples...)
	require.NoError(t, err)
	return m
}

func putProllyMapValues(t *testing.T, ctx context.Context, ns tree.NodeStore, m prolly.Map, data []testTuple) prolly.Map {
	mut := m.Mutate()
	for _, p := range data {
		err := mut.Put(ctx, buildKeyTuple(ns, p.key), buildValTuple(ns, p.v0, p.v1))
		require.NoError(t, err)
	}
	result, err := mut.Map(ctx)
	require.NoError(t, err)
	return result
}

func deleteProllyMapKeys(t *testing.T, ctx context.Context, ns tree.NodeStore, m prolly.Map, keys []int64) prolly.Map {
	if len(keys) == 0 {
		return m
	}
	mut := m.Mutate()
	for _, k := range keys {
		err := mut.Delete(ctx, buildKeyTuple(ns, k))
		require.NoError(t, err)
	}
	result, err := mut.Map(ctx)
	require.NoError(t, err)
	return result
}

func buildAddressMap(t *testing.T, ctx context.Context, ns tree.NodeStore, tables map[string]prolly.Map) prolly.AddressMap {
	am, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)
	editor := am.Editor()
	for name, m := range tables {
		_, err := ns.Write(ctx, m.Node())
		require.NoError(t, err)
		err = editor.Add(ctx, name, m.HashOf())
		require.NoError(t, err)
	}
	am, err = editor.Flush(ctx)
	require.NoError(t, err)
	return am
}

func updateAddressMap(t *testing.T, ctx context.Context, ns tree.NodeStore, am prolly.AddressMap, name string, m prolly.Map) prolly.AddressMap {
	_, err := ns.Write(ctx, m.Node())
	require.NoError(t, err)
	editor := am.Editor()
	err = editor.Update(ctx, name, m.HashOf())
	require.NoError(t, err)
	am, err = editor.Flush(ctx)
	require.NoError(t, err)
	return am
}

func addToAddressMap(t *testing.T, ctx context.Context, ns tree.NodeStore, am prolly.AddressMap, name string, m prolly.Map) prolly.AddressMap {
	_, err := ns.Write(ctx, m.Node())
	require.NoError(t, err)
	editor := am.Editor()
	err = editor.Add(ctx, name, m.HashOf())
	require.NoError(t, err)
	am, err = editor.Flush(ctx)
	require.NoError(t, err)
	return am
}

func deleteFromAddressMap(t *testing.T, ctx context.Context, am prolly.AddressMap, name string) prolly.AddressMap {
	editor := am.Editor()
	err := editor.Delete(ctx, name)
	require.NoError(t, err)
	am, err = editor.Flush(ctx)
	require.NoError(t, err)
	return am
}

func buildRootValue(am prolly.AddressMap) types.SerialMessage {
	builder := flatbuffers.NewBuilder(256)
	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	tablesoff := builder.CreateByteVector(ambytes)
	emptyAddr := make([]byte, hash.ByteLen)
	fkoff := builder.CreateByteVector(emptyAddr)
	serial.RootValueStart(builder)
	serial.RootValueAddTables(builder, tablesoff)
	serial.RootValueAddForeignKeyAddr(builder, fkoff)
	bs := serial.FinishMessage(builder, serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
	return types.SerialMessage(bs)
}

type datasFactory func(context.Context) (tree.NodeStore, datas.Database)

func testPuller(t *testing.T, makeDB datasFactory) {
	ctx := context.Background()
	ns, db := makeDB(ctx)
	defer db.Close()

	type delta struct {
		name       string
		sets       map[string][]testTuple
		deletes    map[string][]int64
		tblDeletes []string
	}

	deltas := []delta{
		{
			name:       "empty",
			sets:       map[string][]testTuple{},
			deletes:    map[string][]int64{},
			tblDeletes: []string{},
		},
		{
			name: "employees",
			sets: map[string][]testTuple{
				"employees": {
					{0, "Hendriks", "Software Engineer"},
					{1, "Sehn", "CEO"},
					{2, "Son", "Software Engineer"},
				},
			},
			deletes:    map[string][]int64{},
			tblDeletes: []string{},
		},
		{
			name: "ip to country",
			sets: map[string][]testTuple{
				"ip_to_country": {
					{0, "5.183.230.1", "BZ"},
					{1, "5.180.188.1", "AU"},
					{2, "2.56.9.244", "GB"},
					{3, "20.175.7.56", "US"},
				},
			},
			deletes:    map[string][]int64{},
			tblDeletes: []string{},
		},
		{
			name: "more ips",
			sets: map[string][]testTuple{
				"ip_to_country": {
					{4, "20.175.193.85", "US"},
					{5, "5.196.110.191", "FR"},
					{6, "4.14.242.160", "CA"},
				},
			},
			deletes:    map[string][]int64{},
			tblDeletes: []string{},
		},
		{
			name: "more employees",
			sets: map[string][]testTuple{
				"employees": {
					{3, "Jesuele", "Software Engineer"},
					{4, "Wilkins", "Software Engineer"},
					{5, "McCulloch", "Software Engineer"},
				},
			},
			deletes:    map[string][]int64{},
			tblDeletes: []string{},
		},
		{
			name:       "delete ips table",
			sets:       map[string][]testTuple{},
			deletes:    map[string][]int64{},
			tblDeletes: []string{"ip_to_country"},
		},
		{
			name: "delete some employees",
			sets: map[string][]testTuple{},
			deletes: map[string][]int64{
				"employees": {0, 1, 2},
			},
			tblDeletes: []string{},
		},
	}

	ds, err := db.GetDataset(ctx, "ds")
	require.NoError(t, err)

	tables := make(map[string]prolly.Map)
	am, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)

	var parent []hash.Hash
	states := map[string]hash.Hash{}
	for _, d := range deltas {
		for tbl, data := range d.sets {
			existing, ok := tables[tbl]
			if !ok {
				m := createProllyMap(t, ctx, ns, data)
				tables[tbl] = m
				am = addToAddressMap(t, ctx, ns, am, tbl, m)
			} else {
				m := putProllyMapValues(t, ctx, ns, existing, data)
				tables[tbl] = m
				am = updateAddressMap(t, ctx, ns, am, tbl, m)
			}
		}

		for tbl, keys := range d.deletes {
			existing, ok := tables[tbl]
			require.True(t, ok, "cannot delete from table that wasn't created")
			m := deleteProllyMapKeys(t, ctx, ns, existing, keys)
			tables[tbl] = m
			am = updateAddressMap(t, ctx, ns, am, tbl, m)
		}

		for _, tbl := range d.tblDeletes {
			delete(tables, tbl)
			am = deleteFromAddressMap(t, ctx, am, tbl)
		}

		rootVal := buildRootValue(am)
		commitOpts := datas.CommitOptions{Parents: parent}
		ds, err = db.Commit(ctx, ds, rootVal, commitOpts)
		require.NoError(t, err)

		dsAddr, ok := ds.MaybeHeadAddr()
		require.True(t, ok)

		parent = []hash.Hash{dsAddr}
		states[d.name] = dsAddr
	}

	bigTable := makeABigProllyTable(t, ctx, ns)
	am = addToAddressMap(t, ctx, ns, am, "big_table", bigTable)

	rootVal := buildRootValue(am)
	commitOpts := datas.CommitOptions{Parents: parent}
	ds, err = db.Commit(ctx, ds, rootVal, commitOpts)
	require.NoError(t, err)

	addr, ok := ds.MaybeHeadAddr()
	require.True(t, ok)
	states["add big table"] = addr

	srcCS := datas.ChunkStoreFromDatabase(db)

	for k, rootAddr := range states {
		t.Run(k, func(t *testing.T) {
			statsCh := make(chan Stats, 16)
			wg := new(sync.WaitGroup)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for evt := range statsCh {
					jsonBytes, err := json.Marshal(evt)
					if err == nil {
						t.Logf("stats: %s\n", string(jsonBytes))
					}
				}
			}()

			_, sinkdb := makeDB(ctx)
			defer sinkdb.Close()

			tmpDir := filepath.Join(os.TempDir(), uuid.New().String())
			err = os.MkdirAll(tmpDir, os.ModePerm)
			require.NoError(t, err)
			waf, err := types.WalkAddrsForChunkStore(srcCS)
			require.NoError(t, err)
			sinkCS := datas.ChunkStoreFromDatabase(sinkdb)
			plr, err := NewPuller(ctx, tmpDir, 1<<20, srcCS, sinkCS, waf, []hash.Hash{rootAddr}, statsCh)
			require.NoError(t, err)

			err = plr.Pull(ctx)
			close(statsCh)
			require.NoError(t, err)
			wg.Wait()

			sinkDS, err := sinkdb.GetDataset(ctx, "ds")
			require.NoError(t, err)
			sinkDS, err = sinkdb.FastForward(ctx, sinkDS, rootAddr, "")
			require.NoError(t, err)

			require.NoError(t, err)
			sinkRootAddr, ok := sinkDS.MaybeHeadAddr()
			require.True(t, ok)

			pullerHashEquality(t, ctx, rootAddr, sinkRootAddr, srcCS, sinkCS)
		})
	}
}

func makeABigProllyTable(t *testing.T, ctx context.Context, ns tree.NodeStore) prolly.Map {
	tuples := make([]val.Tuple, 0, 256*1024*2)
	for i := 0; i < 256*1024; i++ {
		k := buildKeyTuple(ns, int64(i))
		v := buildValTuple(ns, uuid.New().String(), uuid.New().String())
		tuples = append(tuples, k, v)
	}
	m, err := prolly.NewMapFromTuples(ctx, ns, testKeyDesc, testValDesc, tuples...)
	require.NoError(t, err)
	return m
}

func pullerHashEquality(t *testing.T, ctx context.Context, expected, actual hash.Hash, srcCS, sinkCS chunks.ChunkStore) {
	require.Equal(t, expected, actual)

	srcChunk, err := srcCS.Get(ctx, expected)
	require.NoError(t, err)
	require.False(t, srcChunk.IsEmpty())

	sinkChunk, err := sinkCS.Get(ctx, actual)
	require.NoError(t, err)
	require.False(t, sinkChunk.IsEmpty())

	require.Equal(t, srcChunk.Data(), sinkChunk.Data())
}
