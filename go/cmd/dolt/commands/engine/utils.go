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

package engine

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// CollectDBs takes a MultiRepoEnv and creates Database objects from each environment and returns a slice of these
// objects.
func CollectDBs(ctx context.Context, mrEnv *env.MultiRepoEnv, useBulkEditor bool) ([]dsess.SqlDatabase, []filesys.Filesys, error) {
	var dbs []dsess.SqlDatabase
	var locations []filesys.Filesys
	var db dsess.SqlDatabase

	err := mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		db, err = newDatabase(ctx, name, dEnv, useBulkEditor)
		if err != nil {
			return false, err
		}

		dbs = append(dbs, db)
		locations = append(locations, dEnv.FS)

		return false, nil
	})

	if err != nil {
		return nil, nil, err
	}

	return dbs, locations, nil
}

func newDatabase(ctx context.Context, name string, dEnv *env.DoltEnv, useBulkEditor bool) (sqle.Database, error) {
	deaf := dEnv.DbEaFactory(ctx)
	if useBulkEditor {
		deaf = dEnv.BulkDbEaFactory(ctx)
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return sqle.Database{}, err
	}
	opts := editor.Options{
		Deaf:    deaf,
		Tempdir: tmpDir,
	}
	return sqle.NewDatabase(ctx, name, dEnv.DbData(ctx), opts)
}
