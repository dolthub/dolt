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
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

func TestNbsPuller(t *testing.T) {
	testPuller(t, func(ctx context.Context) (types.ValueReadWriter, datas.Database) {
		dir := filepath.Join(os.TempDir(), uuid.New().String())
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		nbf := types.Format_Default.VersionString()
		q := nbs.NewUnlimitedMemQuotaProvider()
		st, err := nbs.NewLocalStore(ctx, nbf, dir, clienttest.DefaultMemTableSize, q, false)
		require.NoError(t, err)

		ns := tree.NewNodeStore(st)
		vs := types.NewValueStore(st)
		return vs, datas.NewTypesDatabase(vs, ns)
	})
}

func TestChunkJournalPuller(t *testing.T) {
	testPuller(t, func(ctx context.Context) (types.ValueReadWriter, datas.Database) {
		dir := filepath.Join(os.TempDir(), uuid.New().String())
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		nbf := types.Format_Default.VersionString()
		q := nbs.NewUnlimitedMemQuotaProvider()

		st, err := nbs.NewLocalJournalingStore(ctx, nbf, dir, q, false)
		require.NoError(t, err)

		ns := tree.NewNodeStore(st)
		vs := types.NewValueStore(st)
		return vs, datas.NewTypesDatabase(vs, ns)
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

		st, err := nbs.NewLocalJournalingStore(ctx, nbf, dir, q, false)
		require.NoError(t, err)

		plr, err := NewPuller(ctx, t.TempDir(), 1<<20, gs, st, waf, []hash.Hash{ghost}, statsCh)
		require.NoError(t, err)
		err = plr.Pull(ctx)
		require.ErrorIs(t, err, nbs.ErrGhostChunkRequested)
	})
}

func addTableValues(ctx context.Context, vrw types.ValueReadWriter, m types.Map, tableName string, alternatingKeyVals ...types.Value) (types.Map, error) {
	val, ok, err := m.MaybeGet(ctx, types.String(tableName))

	if err != nil {
		return types.EmptyMap, err
	}

	var tblMap types.Map
	if ok {
		mv, err := val.(types.Ref).TargetValue(ctx, vrw)

		if err != nil {
			return types.EmptyMap, err
		}

		me := mv.(types.Map).Edit()

		for i := 0; i < len(alternatingKeyVals); i += 2 {
			me.Set(alternatingKeyVals[i], alternatingKeyVals[i+1])
		}

		tblMap, err = me.Map(ctx)

		if err != nil {
			return types.EmptyMap, err
		}
	} else {
		tblMap, err = types.NewMap(ctx, vrw, alternatingKeyVals...)

		if err != nil {
			return types.EmptyMap, err
		}
	}

	tblRef, err := writeValAndGetRef(ctx, vrw, tblMap)

	if err != nil {
		return types.EmptyMap, err
	}

	me := m.Edit()
	me.Set(types.String(tableName), tblRef)
	return me.Map(ctx)
}

func deleteTableValues(ctx context.Context, vrw types.ValueReadWriter, m types.Map, tableName string, keys ...types.Value) (types.Map, error) {
	if len(keys) == 0 {
		return m, nil
	}

	val, ok, err := m.MaybeGet(ctx, types.String(tableName))

	if err != nil {
		return types.EmptyMap, err
	}

	if !ok {
		return types.EmptyMap, errors.New("can't delete from table that wasn't created")
	}

	mv, err := val.(types.Ref).TargetValue(ctx, vrw)

	if err != nil {
		return types.EmptyMap, err
	}

	me := mv.(types.Map).Edit()
	for _, k := range keys {
		me.Remove(k)
	}

	tblMap, err := me.Map(ctx)

	if err != nil {
		return types.EmptyMap, err
	}

	tblRef, err := writeValAndGetRef(ctx, vrw, tblMap)

	if err != nil {
		return types.EmptyMap, err
	}

	me = m.Edit()
	me.Set(types.String(tableName), tblRef)
	return me.Map(ctx)
}

type datasFactory func(context.Context) (types.ValueReadWriter, datas.Database)

func testPuller(t *testing.T, makeDB datasFactory) {
	ctx := context.Background()
	vs, db := makeDB(ctx)
	defer db.Close()

	deltas := []struct {
		name       string
		sets       map[string][]types.Value
		deletes    map[string][]types.Value
		tblDeletes []string
	}{
		{
			"empty",
			map[string][]types.Value{},
			map[string][]types.Value{},
			[]string{},
		},
		{
			"employees",
			map[string][]types.Value{
				"employees": {
					mustTuple(types.NewTuple(vs.Format(), types.String("Hendriks"), types.String("Brian"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Software Engineer"), types.Int(39))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Sehn"), types.String("Timothy"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("CEO"), types.Int(39))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Son"), types.String("Aaron"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Software Engineer"), types.Int(36))),
				},
			},
			map[string][]types.Value{},
			[]string{},
		},
		{
			"ip to country",
			map[string][]types.Value{
				"ip_to_country": {
					types.String("5.183.230.1"), types.String("BZ"),
					types.String("5.180.188.1"), types.String("AU"),
					types.String("2.56.9.244"), types.String("GB"),
					types.String("20.175.7.56"), types.String("US"),
				},
			},
			map[string][]types.Value{},
			[]string{},
		},
		{
			"more ips",
			map[string][]types.Value{
				"ip_to_country": {
					types.String("20.175.193.85"), types.String("US"),
					types.String("5.196.110.191"), types.String("FR"),
					types.String("4.14.242.160"), types.String("CA"),
				},
			},
			map[string][]types.Value{},
			[]string{},
		},
		{
			"more employees",
			map[string][]types.Value{
				"employees": {
					mustTuple(types.NewTuple(vs.Format(), types.String("Jesuele"), types.String("Matt"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Software Engineer"), types.NullValue)),
					mustTuple(types.NewTuple(vs.Format(), types.String("Wilkins"), types.String("Daylon"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Software Engineer"), types.NullValue)),
					mustTuple(types.NewTuple(vs.Format(), types.String("Katie"), types.String("McCulloch"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Software Engineer"), types.NullValue)),
				},
			},
			map[string][]types.Value{},
			[]string{},
		},
		{
			"delete ips table",
			map[string][]types.Value{},
			map[string][]types.Value{},
			[]string{"ip_to_country"},
		},
		{
			"delete some employees",
			map[string][]types.Value{},
			map[string][]types.Value{
				"employees": {
					mustTuple(types.NewTuple(vs.Format(), types.String("Hendriks"), types.String("Brian"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Sehn"), types.String("Timothy"))),
					mustTuple(types.NewTuple(vs.Format(), types.String("Son"), types.String("Aaron"))),
				},
			},
			[]string{},
		},
	}

	ds, err := db.GetDataset(ctx, "ds")
	require.NoError(t, err)
	rootMap, err := types.NewMap(ctx, vs)
	require.NoError(t, err)

	var parent []hash.Hash
	states := map[string]hash.Hash{}
	for _, delta := range deltas {
		for tbl, sets := range delta.sets {
			rootMap, err = addTableValues(ctx, vs, rootMap, tbl, sets...)
			require.NoError(t, err)
		}

		for tbl, dels := range delta.deletes {
			rootMap, err = deleteTableValues(ctx, vs, rootMap, tbl, dels...)
			require.NoError(t, err)
		}

		me := rootMap.Edit()
		for _, tbl := range delta.tblDeletes {
			me.Remove(types.String(tbl))
		}
		rootMap, err = me.Map(ctx)
		require.NoError(t, err)

		commitOpts := datas.CommitOptions{Parents: parent}
		ds, err = db.Commit(ctx, ds, rootMap, commitOpts)
		require.NoError(t, err)

		dsAddr, ok := ds.MaybeHeadAddr()
		require.True(t, ok)

		parent = []hash.Hash{dsAddr}

		states[delta.name] = dsAddr
	}

	tbl, err := makeABigTable(ctx, vs)
	require.NoError(t, err)

	tblRef, err := writeValAndGetRef(ctx, vs, tbl)
	require.NoError(t, err)

	me := rootMap.Edit()
	me.Set(types.String("big_table"), tblRef)
	rootMap, err = me.Map(ctx)
	require.NoError(t, err)

	commitOpts := datas.CommitOptions{Parents: parent}
	ds, err = db.Commit(ctx, ds, rootMap, commitOpts)
	require.NoError(t, err)

	addr, ok := ds.MaybeHeadAddr()
	require.True(t, ok)

	states["add big table"] = addr

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

			sinkvs, sinkdb := makeDB(ctx)
			defer sinkdb.Close()

			tmpDir := filepath.Join(os.TempDir(), uuid.New().String())
			err = os.MkdirAll(tmpDir, os.ModePerm)
			require.NoError(t, err)
			waf, err := types.WalkAddrsForChunkStore(datas.ChunkStoreFromDatabase(db))
			require.NoError(t, err)
			plr, err := NewPuller(ctx, tmpDir, 1<<20, datas.ChunkStoreFromDatabase(db), datas.ChunkStoreFromDatabase(sinkdb), waf, []hash.Hash{rootAddr}, statsCh)
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

			eq, err := pullerAddrEquality(ctx, rootAddr, sinkRootAddr, vs, sinkvs)
			require.NoError(t, err)
			assert.True(t, eq)
		})
	}
}

func makeABigTable(ctx context.Context, vrw types.ValueReadWriter) (types.Map, error) {
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return types.EmptyMap, nil
	}

	me := m.Edit()

	for i := 0; i < 256*1024; i++ {
		tpl, err := types.NewTuple(vrw.Format(), types.UUID(uuid.New()), types.String(uuid.New().String()), types.Float(float64(i)))

		if err != nil {
			return types.EmptyMap, err
		}

		me.Set(types.Int(i), tpl)
	}

	return me.Map(ctx)
}

func pullerAddrEquality(ctx context.Context, expected, actual hash.Hash, src, sink types.ValueReadWriter) (bool, error) {
	if expected != actual {
		return false, nil
	}

	expectedVal, err := src.ReadValue(ctx, expected)
	if err != nil {
		return false, err
	}
	actualVal, err := sink.ReadValue(ctx, actual)
	if err != nil {
		return false, err
	}

	return expectedVal.Equals(actualVal), nil
}

func writeValAndGetRef(ctx context.Context, vrw types.ValueReadWriter, val types.Value) (types.Ref, error) {
	valRef, err := types.NewRef(val, vrw.Format())

	if err != nil {
		return types.Ref{}, err
	}

	targetVal, err := valRef.TargetValue(ctx, vrw)

	if err != nil {
		return types.Ref{}, err
	}

	if targetVal == nil {
		_, err = vrw.WriteValue(ctx, val)

		if err != nil {
			return types.Ref{}, err
		}
	}

	return valRef, err
}

func mustTuple(val types.Tuple, err error) types.Tuple {
	d.PanicIfError(err)
	return val
}
