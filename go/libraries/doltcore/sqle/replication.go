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

package sqle

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func getPushOnWriteHook(ctx context.Context, bThreads *sql.BackgroundThreads, dEnv *env.DoltEnv, logger io.Writer) (doltdb.CommitHook, error) {
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

	rem, ok := remotes.Get(remoteName)
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
	if _, val, ok = sql.SystemVariables.GetGlobal(dsess.AsyncReplication); ok && val == dsess.SysVarTrue {
		return NewAsyncPushOnWriteHook(bThreads, ddb, tmpDir, logger)
	}

	return NewPushOnWriteHook(ddb, tmpDir), nil
}

// GetCommitHooks creates a list of hooks to execute on database commit. Hooks that cannot be created because of an
// error in configuration will not prevent the server from starting, and will instead log errors.
func GetCommitHooks(ctx context.Context, bThreads *sql.BackgroundThreads, dEnv *env.DoltEnv, logger io.Writer) ([]doltdb.CommitHook, error) {
	postCommitHooks := make([]doltdb.CommitHook, 0)

	hook, err := getPushOnWriteHook(ctx, bThreads, dEnv, logger)
	if err != nil {
		path, _ := dEnv.FS.Abs(".")
		logrus.Errorf("error loading replication for database at %s, replication disabled: %v", path, err)
		postCommitHooks = append(postCommitHooks, NewLogHook([]byte(err.Error()+"\n")))
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	for _, h := range postCommitHooks {
		_ = h.SetLogger(ctx, logger)
	}
	return postCommitHooks, nil
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (ReadReplicaDatabase, error) {
	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(ctx),
	}

	db, err := NewDatabase(ctx, name, dEnv.DbData(ctx), opts)
	if err != nil {
		return ReadReplicaDatabase{}, err
	}

	rrd, err := NewReadReplicaDatabase(ctx, db, remoteName, dEnv)
	if err != nil {
		err = fmt.Errorf("%s from remote '%s'; %w", ErrFailedToLoadReplicaDB.Error(), remoteName, err)
		return ReadReplicaDatabase{}, err
	}

	if sqlCtx, ok := ctx.(*sql.Context); ok {
		sqlCtx.GetLogger().Infof(
			"replication enabled for database '%s' from remote '%s'", name, remoteName)
	}

	return rrd, nil
}

func ApplyReplicationConfig(ctx context.Context, bThreads *sql.BackgroundThreads, mrEnv *env.MultiRepoEnv, logger io.Writer, dbs ...dsess.SqlDatabase) ([]dsess.SqlDatabase, error) {
	outputDbs := make([]dsess.SqlDatabase, len(dbs))
	for i, db := range dbs {
		dEnv := mrEnv.GetEnv(db.Name())
		if dEnv == nil {
			outputDbs[i] = db
			continue
		}
		postCommitHooks, err := GetCommitHooks(ctx, bThreads, dEnv, logger)
		if err != nil {
			return nil, err
		}
		dEnv.DoltDB(ctx).SetCommitHooks(ctx, postCommitHooks)

		if _, remote, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaRemote); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return nil, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			rdb, err := newReplicaDatabase(ctx, db.Name(), remoteName, dEnv)
			if err == nil {
				db = rdb
			} else {
				logrus.Errorf("invalid replication configuration, replication disabled: %v", err)
			}
		}

		outputDbs[i] = db
	}
	return outputDbs, nil
}
