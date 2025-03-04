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
	"errors"
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

func getPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv, logger io.Writer) (doltdb.CommitHook, RunAsyncThreads, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateToRemote)
	if !ok {
		return nil, nil, sql.ErrUnknownSystemVariable.New(dsess.ReplicateToRemote)
	} else if val == "" {
		return nil, nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, nil, err
	}

	rem, ok := remotes.Get(remoteName)
	if !ok {
		return nil, nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return nil, nil, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, nil, err
	}
	if _, val, ok = sql.SystemVariables.GetGlobal(dsess.AsyncReplication); ok && val == dsess.SysVarTrue {
		hook, runThreads := NewAsyncPushOnWriteHook(ddb, tmpDir, logger)
		return hook, runThreads, nil
	}

	return NewPushOnWriteHook(ddb, tmpDir), nil, nil
}

type RunAsyncThreads func(*sql.BackgroundThreads, func(context.Context) (*sql.Context, error)) error

// GetCommitHooks creates a list of hooks to execute on database commit. Hooks that cannot be created because of an
// error in configuration will not prevent the server from starting, and will instead log errors.
func GetCommitHooks(ctx context.Context, dEnv *env.DoltEnv, logger io.Writer) ([]doltdb.CommitHook, RunAsyncThreads, error) {
	postCommitHooks := make([]doltdb.CommitHook, 0)
	hook, runThreads, err := getPushOnWriteHook(ctx, dEnv, logger)
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
	return postCommitHooks, runThreads, nil
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

// Converts |db| into a |ReadReplicaDatabase| if read replication is
// configured through sql SystemVariables. This is called both at
// startup, for the entire set of databases, and is called when
// we create new databases through |registerNewDatabases|.
func applyReadReplicationConfigToDatabase(ctx context.Context, dEnv *env.DoltEnv, db dsess.SqlDatabase) (dsess.SqlDatabase, error) {
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
	return db, nil
}

func ApplyReplicationConfig(ctx context.Context, mrEnv *env.MultiRepoEnv, logger io.Writer, dbs ...dsess.SqlDatabase) ([]dsess.SqlDatabase, RunAsyncThreads, error) {
	outputDbs := make([]dsess.SqlDatabase, len(dbs))
	asyncRunners := make([]RunAsyncThreads, len(dbs))
	for i, db := range dbs {
		dEnv := mrEnv.GetEnv(db.Name())
		if dEnv == nil {
			outputDbs[i] = db
			continue
		}
		postCommitHooks, runAsyncThreads, err := GetCommitHooks(ctx, dEnv, logger)
		if err != nil {
			return nil, nil, err
		}
		dEnv.DoltDB(ctx).PrependCommitHooks(ctx, postCommitHooks...)

		outputDbs[i], err = applyReadReplicationConfigToDatabase(ctx, dEnv, db)
		if err != nil {
			return nil, nil, err
		}

		asyncRunners[i] = runAsyncThreads
	}
	runAsyncThreads := func(bThreads *sql.BackgroundThreads, ctxF func(context.Context) (*sql.Context, error)) error {
		var err error
		for _, f := range asyncRunners {
			if f != nil {
				err = errors.Join(err, f(bThreads, ctxF))
			}
		}
		return err
	}
	return outputDbs, runAsyncThreads, nil
}
