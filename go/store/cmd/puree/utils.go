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

package main

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func CollectMaps(ctx context.Context, dir, branch, table string) (maps map[string]types.Map, vrw types.ValueReadWriter, err error) {
	root, err := GetRootVal(ctx, dir, branch)
	if err != nil {
		return nil, nil, err
	}

	if table != "" {
		t, name, ok, err := root.GetTableInsensitive(ctx, table)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("table %s not found", table)
		}
		table = name

		m, err := t.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}

		return map[string]types.Map{table: m}, root.VRW(), nil
	}

	maps = make(map[string]types.Map)
	err = root.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		m, err := table.GetRowData(ctx)
		if err != nil {
			return true, err
		}
		maps[name] = m
		return
	})
	if err != nil {
		return nil, nil, err
	}

	return maps, root.VRW(), nil
}

func GetRootVal(ctx context.Context, dir, branch string) (*doltdb.RootValue, error) {
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, "file://"+dir+"/.dolt/noms", "0.0.0-test_tuples")
	db := dEnv.DoltDB
	c, err := db.ResolveCommitRef(ctx, ref.NewBranchRef(branch))
	if err != nil {
		return nil, err
	}
	return c.GetRootValue()
}

func RewriteMapWithStats(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (types.Map, RewriteStats, error) {
	stats := RewriteStats{
		before: &Hist{},
		after:  &Hist{},
	}

	stats.before = getChunkSizes(ctx, vrw, m)

	m2, _ := types.NewMap(ctx, vrw)
	me := m2.Edit()

	err := m.IterAll(ctx, func(key, val types.Value) (err error) {
		me.Set(key, val)
		return
	})
	if err != nil {
		return types.EmptyMap, stats, err
	}

	m2, err = me.Map(ctx)
	if err != nil {
		return types.EmptyMap, stats, err
	}
	_, err = vrw.WriteValue(ctx, m2)
	if err != nil {
		return types.EmptyMap, stats, err
	}

	stats.after = getChunkSizes(ctx, vrw, m2)

	return m2, stats, nil
}

type RewriteStats struct {
	before, after *Hist
}

func getChunkSizes(ctx context.Context, vrw types.ValueReadWriter, m types.Map) *Hist {
	r, err := types.NewRef(m, m.Format())
	if err != nil {
		panic(err)
	}

	h := &Hist{}
	if err := walkMap(ctx, []types.Ref{r}, vrw, hash.NewHashSet(), h); err != nil {
		panic(err)
	}

	return h
}

func walkMap(ctx context.Context, rs []types.Ref, vrw types.ValueReadWriter, hs hash.HashSet, chunksizes *Hist) error {
	res := make([]types.Ref, 0)
	next := rs
	for len(next) > 0 {
		cur := next
		next = make([]types.Ref, 0)
		for _, r := range cur {
			hs.Insert(r.TargetHash())
			if r.Height() == 1 {
				res = append(res, r)
				continue
			}
			v, err := r.TargetValue(ctx, vrw)
			if err != nil {
				return err
			}
			m := v.(types.Map)
			chunksizes.add(int(m.EncodedLen()))
			err = m.WalkRefs(m.Format(), func(r types.Ref) error {
				next = append(next, r)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
