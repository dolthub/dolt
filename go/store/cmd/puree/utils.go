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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/store/types"
)

func CollectMaps(ctx context.Context, dir, branch, table string) (maps map[string]types.Map, err error) {
	root, err := GetRootVal(ctx, dir, branch)
	if err != nil {
		return nil, err
	}

	if table != "" {
		t, ok, err := root.GetTable(ctx, table)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("table %s not found", table)
		}

		m, err := t.GetRowData(ctx)
		if err != nil {
			return nil, err
		}

		return map[string]types.Map{table: m}, nil
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
		return nil, err
	}

	return maps, nil
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
