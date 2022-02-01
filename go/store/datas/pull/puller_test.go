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
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

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

func tempDirDB(ctx context.Context) (types.ValueReadWriter, datas.Database, error) {
	dir := filepath.Join(os.TempDir(), uuid.New().String())
	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		return nil, nil, err
	}

	st, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), dir, clienttest.DefaultMemTableSize)
	if err != nil {
		return nil, nil, err
	}

	vs := types.NewValueStore(st)

	return vs, datas.NewTypesDatabase(vs), nil
}

func TestPuller(t *testing.T) {
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
					mustTuple(types.NewTuple(types.Format_Default, types.String("Hendriks"), types.String("Brian"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Software Engineer"), types.Int(39))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Sehn"), types.String("Timothy"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("CEO"), types.Int(39))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Son"), types.String("Aaron"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Software Engineer"), types.Int(36))),
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
					mustTuple(types.NewTuple(types.Format_Default, types.String("Jesuele"), types.String("Matt"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Software Engineer"), types.NullValue)),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Wilkins"), types.String("Daylon"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Software Engineer"), types.NullValue)),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Katie"), types.String("McCulloch"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Software Engineer"), types.NullValue)),
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
					mustTuple(types.NewTuple(types.Format_Default, types.String("Hendriks"), types.String("Brian"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Sehn"), types.String("Timothy"))),
					mustTuple(types.NewTuple(types.Format_Default, types.String("Son"), types.String("Aaron"))),
				},
			},
			[]string{},
		},
	}

	ctx := context.Background()
	vs, db, err := tempDirDB(ctx)
	require.NoError(t, err)
	ds, err := db.GetDataset(ctx, "ds")
	require.NoError(t, err)
	rootMap, err := types.NewMap(ctx, vs)
	require.NoError(t, err)

	parent, err := types.NewList(ctx, vs)
	require.NoError(t, err)
	states := map[string]types.Ref{}
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

		commitOpts := datas.CommitOptions{ParentsList: parent}
		ds, err = db.Commit(ctx, ds, rootMap, commitOpts)
		require.NoError(t, err)

		r, ok, err := ds.MaybeHeadRef()
		require.NoError(t, err)
		require.True(t, ok)

		parent, err = types.NewList(ctx, vs, r)
		require.NoError(t, err)

		states[delta.name] = r
	}

	tbl, err := makeABigTable(ctx, vs)
	require.NoError(t, err)

	tblRef, err := writeValAndGetRef(ctx, vs, tbl)
	require.NoError(t, err)

	me := rootMap.Edit()
	me.Set(types.String("big_table"), tblRef)
	rootMap, err = me.Map(ctx)
	require.NoError(t, err)

	commitOpts := datas.CommitOptions{ParentsList: parent}
	ds, err = db.Commit(ctx, ds, rootMap, commitOpts)
	require.NoError(t, err)

	r, ok, err := ds.MaybeHeadRef()
	require.NoError(t, err)
	require.True(t, ok)

	states["add big table"] = r

	for k, rootRef := range states {
		t.Run(k, func(t *testing.T) {
			eventCh := make(chan PullerEvent, 128)
			wg := new(sync.WaitGroup)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for evt := range eventCh {
					var details interface{}
					switch evt.EventType {
					case NewLevelTWEvent, DestDBHasTWEvent, LevelUpdateTWEvent:
						details = evt.TWEventDetails
					default:
						details = evt.TFEventDetails
					}

					jsonBytes, err := json.Marshal(details)

					if err == nil {
						t.Logf("event_type: %d details: %s\n", evt.EventType, string(jsonBytes))
					}
				}
			}()

			sinkvs, sinkdb, err := tempDirDB(ctx)
			require.NoError(t, err)

			tmpDir := filepath.Join(os.TempDir(), uuid.New().String())
			err = os.MkdirAll(tmpDir, os.ModePerm)
			require.NoError(t, err)
			wrf, err := types.WalkRefsForChunkStore(datas.ChunkStoreFromDatabase(db))
			require.NoError(t, err)
			plr, err := NewPuller(ctx, tmpDir, 128, datas.ChunkStoreFromDatabase(db), datas.ChunkStoreFromDatabase(sinkdb), wrf, rootRef.TargetHash(), eventCh)
			require.NoError(t, err)

			err = plr.Pull(ctx)
			close(eventCh)
			require.NoError(t, err)
			wg.Wait()

			sinkDS, err := sinkdb.GetDataset(ctx, "ds")
			require.NoError(t, err)
			sinkDS, err = sinkdb.FastForward(ctx, sinkDS, rootRef)
			require.NoError(t, err)

			require.NoError(t, err)
			sinkRootRef, ok, err := sinkDS.MaybeHeadRef()
			require.NoError(t, err)
			require.True(t, ok)

			eq, err := pullerRefEquality(ctx, rootRef, sinkRootRef, vs, sinkvs)
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

func pullerRefEquality(ctx context.Context, expectad, actual types.Ref, src, sink types.ValueReadWriter) (bool, error) {
	expectedVal, err := expectad.TargetValue(ctx, src)

	if err != nil {
		return false, err
	}

	actualVal, err := actual.TargetValue(ctx, sink)
	if err != nil {
		return false, err
	}

	exPs, exTbls, err := parentsAndTables(expectedVal.(types.Struct))
	if err != nil {
		return false, err
	}

	actPs, actTbls, err := parentsAndTables(actualVal.(types.Struct))
	if err != nil {
		return false, err
	}

	if !exPs.Equals(actPs) {
		return false, nil
	}

	err = exTbls.IterAll(ctx, func(key, exVal types.Value) error {
		actVal, ok, err := actTbls.MaybeGet(ctx, key)

		if err != nil {
			return err
		}

		if !ok {
			return errors.New("Missing table " + string(key.(types.String)))
		}

		exMapVal, err := exVal.(types.Ref).TargetValue(ctx, src)

		if err != nil {
			return err
		}

		actMapVal, err := actVal.(types.Ref).TargetValue(ctx, sink)

		if err != nil {
			return err
		}

		return errIfNotEqual(ctx, exMapVal.(types.Map), actMapVal.(types.Map))
	})

	if err != nil {
		return false, err
	}

	return exTbls.Equals(actTbls), nil
}

var errNotEqual = errors.New("not equal")

func errIfNotEqual(ctx context.Context, ex, act types.Map) error {
	exItr, err := ex.Iterator(ctx)

	if err != nil {
		return err
	}

	actItr, err := act.Iterator(ctx)

	if err != nil {
		return err
	}

	for {
		exK, exV, err := exItr.Next(ctx)

		if err != nil {
			return err
		}

		actK, actV, err := actItr.Next(ctx)

		if err != nil {
			return err
		}

		if actK == nil && exK == nil {
			break
		} else if exK == nil || actK == nil {
			return errNotEqual
		}

		if exV == nil && actV == nil {
			continue
		} else if exV == nil || actV == nil {
			return errNotEqual
		}

		if !exK.Equals(actK) || !exV.Equals(actV) {
			return errNotEqual
		}
	}

	return nil
}

func parentsAndTables(cm types.Struct) (types.List, types.Map, error) {
	ps, ok, err := cm.MaybeGet(datas.ParentsListField)

	if err != nil {
		return types.EmptyList, types.EmptyMap, err
	}

	if !ok {
		return types.EmptyList, types.EmptyMap, err
	}

	tbls, ok, err := cm.MaybeGet("value")

	if err != nil {
		return types.EmptyList, types.EmptyMap, err
	}

	if !ok {
		return types.EmptyList, types.EmptyMap, err
	}

	return ps.(types.List), tbls.(types.Map), nil
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
