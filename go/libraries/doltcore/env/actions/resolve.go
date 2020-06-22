// Copyright 2019 Liquidata, Inc.
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

package actions

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/merge"
)

type AutoResolveStats struct {
}

func AutoResolveAll(ctx context.Context, dEnv *env.DoltEnv, autoResolver merge.AutoResolver) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	tbls, err := root.TablesInConflict(ctx)

	if err != nil {
		return err
	}

	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func AutoResolveTables(ctx context.Context, dEnv *env.DoltEnv, autoResolver merge.AutoResolver, tbls []string) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func autoResolve(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, autoResolver merge.AutoResolver, tbls []string) error {
	tableEditSession := doltdb.CreateTableEditSession(root, doltdb.TableEditSessionProps{})

	for _, tblName := range tbls {
		tbl, ok, err := root.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		if !ok {
			return doltdb.ErrTableNotFound
		}

		err = merge.ResolveTable(ctx, root.VRW(), tblName, tbl, autoResolver, tableEditSession)

		if err != nil {
			return err
		}
	}

	newRoot, err := tableEditSession.GetRoot(ctx)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}
