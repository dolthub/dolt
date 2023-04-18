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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

// CollectDBs takes a MultiRepoEnv and creates Database objects from each environment and returns a slice of these
// objects.
func CollectDBs(ctx context.Context, mrEnv *env.MultiRepoEnv, useBulkEditor bool) ([]sqle.SqlDatabase, []filesys.Filesys, error) {
	var dbs []sqle.SqlDatabase
	var locations []filesys.Filesys
	var db sqle.SqlDatabase

	err := mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		postCommitHooks, err := GetCommitHooks(ctx, dEnv)
		if err != nil {
			return true, err
		}
		dEnv.DoltDB.SetCommitHooks(ctx, postCommitHooks)

		db, err = newDatabase(ctx, name, dEnv, useBulkEditor)
		if err != nil {
			return false, err
		}

		if _, remote, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaRemote); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return true, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			db, err = newReplicaDatabase(ctx, name, remoteName, dEnv)
			if err != nil {
				return true, err
			}
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

// GetCommitHooks creates a list of hooks to execute on database commit. If doltdb.SkipReplicationErrorsKey is set,
// replace misconfigured hooks with doltdb.LogHook instances that prints a warning when trying to execute.
// TODO: this duplicates code in the sqle package
func GetCommitHooks(ctx context.Context, dEnv *env.DoltEnv) ([]doltdb.CommitHook, error) {
	postCommitHooks := make([]doltdb.CommitHook, 0)

	if hook, err := getPushOnWriteHook(ctx, dEnv); err != nil {
		path, _ := dEnv.FS.Abs(".")
		err = fmt.Errorf("failure loading hook for database at %s; %w", path, err)
		if dsess.IgnoreReplicationErrors() {
			postCommitHooks = append(postCommitHooks, doltdb.NewLogHook([]byte(err.Error()+"\n")))
		} else {
			return nil, err
		}
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	return postCommitHooks, nil
}

func newDatabase(ctx context.Context, name string, dEnv *env.DoltEnv, useBulkEditor bool) (sqle.Database, error) {
	deaf := dEnv.DbEaFactory()
	if useBulkEditor {
		deaf = dEnv.BulkDbEaFactory()
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return sqle.Database{}, err
	}
	opts := editor.Options{
		Deaf:    deaf,
		Tempdir: tmpDir,
	}
	return sqle.NewDatabase(ctx, name, dEnv.DbData(), opts)
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (sqle.ReadReplicaDatabase, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return sqle.ReadReplicaDatabase{}, err
	}
	opts := editor.Options{
		Deaf:    dEnv.DbEaFactory(),
		Tempdir: tmpDir,
	}

	db, err := sqle.NewDatabase(ctx, name, dEnv.DbData(), opts)
	if err != nil {
		return sqle.ReadReplicaDatabase{}, err
	}

	rrd, err := sqle.NewReadReplicaDatabase(ctx, db, remoteName, dEnv)
	if err != nil {
		err = fmt.Errorf("%w from remote '%s'; %s", sqle.ErrFailedToLoadReplicaDB, remoteName, err.Error())
		if !dsess.IgnoreReplicationErrors() {
			return sqle.ReadReplicaDatabase{}, err
		}
		cli.Println(err)
		return sqle.ReadReplicaDatabase{Database: db}, nil
	}
	return rrd, nil
}

func getPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.PushOnWriteHook, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateToRemote)
	if !ok {
		return nil, sql.ErrUnknownSystemVariable.New(dsess.ReplicateToRemote)
	} else if val == "" {
		return nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes[remoteName]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return nil, err
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	pushHook := doltdb.NewPushOnWriteHook(ddb, tmpDir)
	return pushHook, nil
}
